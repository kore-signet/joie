use std::marker::PhantomData;

use memchr::memmem::Finder;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use rkyv::Archive;
use storage::Storage;
use yoke::Yoke;

use crate::{
    highlight::Highlighter,
    id_list::SentenceIdList,
    searcher::{SearchEngine, SearchResult},
    sentence::{ArchivedSentence, SentenceId, SentenceRange},
    DocumentMetadata, SentenceMetadata,
};

use super::{CallerType, DocumentFilter, Query};

#[derive(Clone)]
pub struct PhraseQuery<'a, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    pub(crate) phrase: &'a [u32],
    pub(crate) highlighter: PhraseHighlighter<'a>,
    pub(crate) document_filter: DF,
    pub(crate) spooky: PhantomData<(D, S)>,
}

impl<'b, D, S, DF> Query<D, S> for PhraseQuery<'b, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D> + Sync + Send,
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>, _caller: CallerType) -> SentenceIdList {
        let mut term_sets: Vec<&[SentenceId]> = self
            .phrase
            .par_iter()
            .map(|term| db.index.get(term).unwrap_or(&[]))
            .collect();

        if term_sets.is_empty() {
            return SentenceIdList { ids: Vec::new() };
        }

        term_sets.sort_by_key(|v| v.len());

        let mut sentence_ids = SentenceIdList::from_slice(term_sets[0]);

        match DF::needed() {
            true if term_sets.len() > 1 => {
                for set in &term_sets[1..] {
                    sentence_ids.retain(|v| {
                        self.document_filter
                            .filter_document(unsafe { db.doc_meta.get_unchecked(v.doc as usize) })
                            && set.binary_search(v).is_ok()
                    });
                }
            }
            false if term_sets.len() > 1 => {
                for set in &term_sets[1..] {
                    sentence_ids.retain(|v| set.binary_search(v).is_ok());
                }
            }
            true => {
                sentence_ids.retain(|id| {
                    self.document_filter
                        .filter_document(unsafe { db.doc_meta.get_unchecked(id.doc as usize) })
                });
            }
            false => {}
        }

        sentence_ids
    }

    fn filter_map(&self, result: &mut SearchResult<'_, S>) -> bool {
        result.highlighted_parts = self.highlighter.highlight(result.sentence);
        !result.highlighted_parts.is_empty()
    }

    fn find_highlights(&self, _sentence: &mut SearchResult<'_, S>) {
        // already highlighted by filter_map
    }
}

pub struct YokedPhraseQuery<
    D: DocumentMetadata + 'static,
    S: SentenceMetadata + 'static,
    DF: DocumentFilter<D> + 'static,
> {
    pub inner: Yoke<PhraseQuery<'static, D, S, DF>, Vec<u32>>,
}

impl<D: DocumentMetadata, S: SentenceMetadata, DF: DocumentFilter<D>> Query<D, S>
    for YokedPhraseQuery<D, S, DF>
{
    fn find_sentence_ids(
        &self,
        db: &crate::searcher::SearchEngine<D, S>,
        caller: CallerType,
    ) -> crate::id_list::SentenceIdList {
        self.inner.get().find_sentence_ids(db, caller)
    }

    fn find_highlights(&self, sentence: &mut crate::searcher::SearchResult<'_, S>) {
        self.inner.get().find_highlights(sentence)
    }

    fn filter_map(&self, result: &mut crate::searcher::SearchResult<'_, S>) -> bool {
        self.inner.get().filter_map(result)
    }
}

#[derive(Clone)]
pub struct PhraseHighlighter<'a> {
    phrase: &'a [u32],
    finder: Finder<'a>,
}

impl<'a> PhraseHighlighter<'a> {
    pub fn new(phrase: &'a [u32]) -> PhraseHighlighter<'a> {
        PhraseHighlighter {
            phrase,
            finder: Finder::new(bytemuck::cast_slice(phrase)),
        }
    }
}

impl<'a> Highlighter<'a> for PhraseHighlighter<'a> {
    fn highlight<'b, S: Archive>(
        &'a self,
        sentence: &'b ArchivedSentence<S>,
    ) -> Vec<SentenceRange> {
        let mut highlights = Vec::with_capacity(8);

        let term_bytes: &[u8] = bytemuck::cast_slice(&sentence.terms);

        for idx in self.finder.find_iter(term_bytes) {
            let idx = idx / 4;
            let start_token = &sentence.tokens[idx];
            let end_token = &sentence.tokens[idx + self.phrase.len() - 1];

            highlights.push(SentenceRange {
                start: start_token.start as usize,
                end: end_token.end as usize,
            });
        }

        highlights
    }
}

/* YOKING TRICKS */
unsafe impl<'a, D, S, DF> yoke::Yokeable<'a> for PhraseQuery<'static, D, S, DF>
where
    D: DocumentMetadata + 'static,
    S: SentenceMetadata + 'static,
    DF: DocumentFilter<D> + 'static,
{
    type Output = PhraseQuery<'a, D, S, DF>;
    #[inline]
    fn transform(&'a self) -> &'a Self::Output {
        self
    }
    #[inline]
    fn transform_owned(self) -> Self::Output {
        self
    }
    #[inline]
    unsafe fn make(this: Self::Output) -> Self {
        use core::{mem, ptr};
        debug_assert!(mem::size_of::<Self::Output>() == mem::size_of::<Self>());
        let ptr: *const Self = (&this as *const Self::Output).cast();
        #[allow(forgetting_copy_types)]
        mem::forget(this);
        ptr::read(ptr)
    }
    #[inline]
    fn transform_mut<F>(&'a mut self, f: F)
    where
        F: 'static + for<'b> FnOnce(&'b mut Self::Output),
    {
        unsafe {
            f(core::mem::transmute::<&'a mut Self, &'a mut Self::Output>(
                self,
            ))
        }
    }
}
