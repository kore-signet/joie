use std::marker::PhantomData;

use bytemuck::Pod;
use rkyv::Archive;
use smallvec::SmallVec;
use stable_vec::{core::BitVecCore, StableVec, StableVecFacade};
use storage::Storage;

use crate::{
    highlight::{Highlighter, KeywordHighlighter, PhraseHighlighter},
    searcher::SearchEngine,
    sentence::{ArchivedSentence, SentenceId, SentencePart},
};

#[repr(transparent)]
pub struct StableVecIdIter<'a> {
    inner: <StableVecFacade<&'a SentenceId, BitVecCore<&'a SentenceId>> as IntoIterator>::IntoIter,
}

impl<'a> StableVecIdIter<'a> {
    pub fn new(vec: StableVec<&'a SentenceId>) -> StableVecIdIter<'a> {
        // let v = vec.into_iter().copied();
        StableVecIdIter {
            inner: vec.into_iter(),
        }
    }
}

impl<'a> Iterator for StableVecIdIter<'a> {
    type Item = SentenceId;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| *v)
    }
}

#[inline(always)]
pub const fn always_true<S: Archive>(_sentence: &ArchivedSentence<S>) -> bool {
    true
}

pub trait SentenceFilter<S: Archive> {
    fn filter_sentence(&self, sentence: &ArchivedSentence<S>) -> bool;
}

pub trait DocumentFilter<D: Pod> {
    fn filter_document(&self, document_meta: &D) -> bool;
}

impl<T, D: Pod> DocumentFilter<D> for T
where
    T: Fn(&D) -> bool,
{
    #[inline(always)]
    fn filter_document(&self, document_meta: &D) -> bool {
        self(document_meta)
    }
}

impl<T, S: Archive> SentenceFilter<S> for T
where
    T: Fn(&ArchivedSentence<S>) -> bool,
{
    #[inline(always)]
    fn filter_sentence(&self, sentence: &ArchivedSentence<S>) -> bool {
        self(sentence)
    }
}

pub trait Query<D: Pod, S: Archive> {
    type Ids<'a>: Iterator<Item = SentenceId>
    where
        S: 'a;

    fn find_sentence_ids<'a>(&self, db: &'a SearchEngine<D, S>) -> Self::Ids<'a>;

    #[inline(always)]
    fn filter_sentence(&self, _sentence: &ArchivedSentence<S>) -> bool {
        true
    }

    fn highlight<'a>(
        &self,
        sentence: &'a ArchivedSentence<S>,
    ) -> Option<SmallVec<[SentencePart<'a>; 8]>>;
}

#[derive(Clone, Copy)]
pub struct QueryBuilder<'a, D, S, DF, SF>
where
    D: Pod,
    S: Archive,
    DF: DocumentFilter<D>,
    SF: SentenceFilter<S>,
{
    terms: &'a [u32],
    document_filter: Option<DF>,
    sentence_filter: SF,
    spooky: PhantomData<(D, S)>,
}

impl<'a, D: Pod, S: Archive>
    QueryBuilder<'a, D, S, fn(&D) -> bool, fn(&ArchivedSentence<S>) -> bool>
{
    pub fn start(
        phrase: &'a [u32],
    ) -> QueryBuilder<'a, D, S, fn(&D) -> bool, fn(&ArchivedSentence<S>) -> bool> {
        QueryBuilder {
            terms: phrase,
            document_filter: None,
            sentence_filter: always_true,
            spooky: PhantomData,
        }
    }
}

impl<'a, D: Pod, S: Archive, DF: DocumentFilter<D>, SF: SentenceFilter<S>>
    QueryBuilder<'a, D, S, DF, SF>
{
    pub fn filter_documents<NDF: DocumentFilter<D>>(
        self,
        doc_filter: NDF,
    ) -> QueryBuilder<'a, D, S, NDF, SF> {
        QueryBuilder {
            terms: self.terms,
            document_filter: Some(doc_filter),
            sentence_filter: self.sentence_filter,
            spooky: PhantomData,
        }
    }

    pub fn filter_sentences<NSF: SentenceFilter<S>>(
        self,
        sentence_filter: NSF,
    ) -> QueryBuilder<'a, D, S, DF, NSF> {
        QueryBuilder {
            terms: self.terms,
            document_filter: self.document_filter,
            sentence_filter,
            spooky: PhantomData,
        }
    }

    pub fn phrases(self) -> PhraseQuery<'a, D, S, DF, SF> {
        PhraseQuery {
            phrase: self.terms,
            highlighter: PhraseHighlighter::new(self.terms),
            document_filter: self.document_filter,
            sentence_filter: self.sentence_filter,
            spooky: PhantomData,
        }
    }

    pub fn keywords(self) -> KeywordsQuery<'a, D, S, DF, SF> {
        KeywordsQuery {
            keywords: self.terms,
            highlighter: KeywordHighlighter::new(self.terms),
            document_filter: self.document_filter,
            sentence_filter: self.sentence_filter,
            spooky: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct PhraseQuery<'a, D, S, DF, SF>
where
    D: Pod,
    S: Archive,
    DF: DocumentFilter<D>,
    SF: SentenceFilter<S>,
{
    phrase: &'a [u32],
    highlighter: PhraseHighlighter<'a>,
    document_filter: Option<DF>,
    sentence_filter: SF,
    spooky: PhantomData<(D, S)>,
}

impl<'b, D, S, DF, SF> Query<D, S> for PhraseQuery<'b, D, S, DF, SF>
where
    D: Pod,
    S: Archive,
    DF: DocumentFilter<D>,
    SF: SentenceFilter<S>,
{
    type Ids<'a> = StableVecIdIter<'a> where S: 'a;

    fn find_sentence_ids<'a>(&self, db: &'a SearchEngine<D, S>) -> Self::Ids<'a> {
        let mut term_sets = Vec::with_capacity(self.phrase.len());
        for term in self.phrase {
            let ids = db.index.get(term).unwrap_or(&[]);
            term_sets.push(ids);
        }

        term_sets.sort_by_key(|v| v.len());

        let mut sentence_ids = StableVec::from_iter(term_sets[0].iter());

        match &self.document_filter {
            Some(filter) if term_sets.len() > 1 => {
                for set in &term_sets[1..] {
                    sentence_ids.retain(|v| {
                        filter.filter_document(unsafe { db.doc_meta.get_unchecked(v.doc as usize) })
                            && set.binary_search(v).is_ok()
                    });
                }
            }
            None if term_sets.len() > 1 => {
                for set in &term_sets[1..] {
                    sentence_ids.retain(|v| set.binary_search(v).is_ok());
                }
            }
            Some(filter) => {
                sentence_ids.retain(|id| {
                    filter.filter_document(unsafe { db.doc_meta.get_unchecked(id.doc as usize) })
                });
            }
            None => {}
        }

        StableVecIdIter::new(sentence_ids)
    }

    #[inline(always)]
    fn filter_sentence(&self, sentence: &ArchivedSentence<S>) -> bool {
        self.sentence_filter.filter_sentence(sentence)
    }

    #[inline(always)]
    fn highlight<'a>(
        &self,
        sentence: &'a ArchivedSentence<S>,
    ) -> Option<SmallVec<[SentencePart<'a>; 8]>> {
        self.highlighter.highlight(sentence)
    }
}

#[derive(Clone, Copy)]
pub struct KeywordsQuery<'a, D, S, DF, SF>
where
    D: Pod,
    S: Archive,
    DF: DocumentFilter<D>,
    SF: SentenceFilter<S>,
{
    keywords: &'a [u32],
    highlighter: KeywordHighlighter<'a>,
    document_filter: Option<DF>,
    sentence_filter: SF,
    spooky: PhantomData<(D, S)>,
}

impl<'b, D, S, DF, SF> Query<D, S> for KeywordsQuery<'b, D, S, DF, SF>
where
    D: Pod,
    S: Archive,
    DF: DocumentFilter<D>,
    SF: SentenceFilter<S>,
{
    type Ids<'a> = <Vec<SentenceId> as IntoIterator>::IntoIter where S: 'a;
    fn find_sentence_ids<'a>(&self, db: &'a SearchEngine<D, S>) -> Self::Ids<'a> {
        let mut ids: Vec<SentenceId> = Vec::new();

        for term in self.keywords {
            let set = db.index.get(term).unwrap_or(&[]);

            let dst_len = ids.len();
            let src_len = set.len();

            ids.reserve(src_len);

            unsafe {
                let dst_ptr = ids.as_mut_ptr().add(dst_len);
                std::ptr::copy_nonoverlapping(set.as_ptr(), dst_ptr, src_len);
                ids.set_len(dst_len + src_len);
            }
        }

        ids.sort_unstable();
        ids.dedup();

        if let Some(filter) = &self.document_filter {
            ids.retain(|id| {
                filter.filter_document(unsafe { db.doc_meta.get_unchecked(id.doc as usize) })
            });
        }

        ids.into_iter()
    }

    #[inline(always)]
    fn filter_sentence(&self, sentence: &ArchivedSentence<S>) -> bool {
        self.sentence_filter.filter_sentence(sentence)
    }

    #[inline(always)]
    fn highlight<'a>(
        &self,
        sentence: &'a ArchivedSentence<S>,
    ) -> Option<SmallVec<[SentencePart<'a>; 8]>> {
        self.highlighter.highlight(sentence)
    }
}
