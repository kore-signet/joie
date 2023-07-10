use std::marker::PhantomData;

use bytemuck::Pod;
use rayon::slice::ParallelSliceMut;
use rkyv::Archive;

use storage::Storage;

use crate::{
    highlight::{Highlighter, KeywordHighlighter, PhraseHighlighter},
    id_list::{SentenceIdList, SentenceIdListIter},
    searcher::{SearchEngine, SearchResult},
    sentence::{ArchivedSentence, SentenceId},
};

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

    fn filter_map<'a>(&self, result: SearchResult<'a, S>) -> Option<SearchResult<'a, S>> {
        Some(result)
    }

    fn find_highlights(&self, sentence: &mut SearchResult<'_, S>);
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
    D: Pod + Send + Sync,
    S: Archive + Send + Sync,
    DF: DocumentFilter<D> + Sync + Send,
    SF: SentenceFilter<S>,
{
    type Ids<'a> = SentenceIdListIter where S: 'a;

    fn find_sentence_ids<'a>(&self, db: &'a SearchEngine<D, S>) -> Self::Ids<'a> {
        let mut term_sets = Vec::with_capacity(self.phrase.len());
        for term in self.phrase {
            let ids = db.index.get(term).unwrap_or(&[]);
            term_sets.push(ids);
        }

        term_sets.sort_by_key(|v| v.len());

        let mut sentence_ids = SentenceIdList::from_slice(term_sets[0]);

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

        sentence_ids.into_iter()
    }

    fn filter_map<'a>(&self, mut result: SearchResult<'a, S>) -> Option<SearchResult<'a, S>> {
        result.highlighted_parts = self.highlighter.highlight(result.sentence);
        if result.highlighted_parts.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn find_highlights(&self, _sentence: &mut SearchResult<'_, S>) {
        // already highlighted by filter_map
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

        ids.par_sort();
        ids.dedup();
        // assert!(ids.len() < l1);

        if let Some(filter) = &self.document_filter {
            ids.retain(|id| {
                filter.filter_document(unsafe { db.doc_meta.get_unchecked(id.doc as usize) })
            });
        }

        ids.into_iter()
    }

    #[inline(always)]
    fn find_highlights(&self, result: &mut SearchResult<'_, S>) {
        result.highlighted_parts = self.highlighter.highlight(result.sentence);
    }
}