use std::marker::PhantomData;

use rayon::slice::ParallelSliceMut;
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

#[derive(Clone, Copy)]
pub struct KeywordsQuery<'a, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    pub(crate) keywords: &'a [u32],
    pub(crate) highlighter: KeywordHighlighter<'a>,
    pub(crate) document_filter: DF,
    pub(crate) spooky: PhantomData<(D, S)>,
}

impl<'b, D, S, DF> Query<D, S> for KeywordsQuery<'b, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>, caller: CallerType) -> SentenceIdList {
        let mut ids = if let [lhs, rhs] = self.keywords {
            let (lhs, rhs) = (
                db.index.get(lhs).unwrap_or(&[]),
                db.index.get(rhs).unwrap_or(&[]),
            );

            let mut ids = SentenceIdList::merge_slices(lhs, rhs);
            if !caller.intersect() {
                ids.ids.dedup();
            }

            ids
        } else {
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
            SentenceIdList { ids }
        };

        //     ids
        // };

        if DF::needed() {
            ids.retain(|id| {
                self.document_filter
                    .filter_document(unsafe { db.doc_meta.get_unchecked(id.doc as usize) })
            });
        }

        ids
    }

    #[inline(always)]
    fn find_highlights(&self, result: &mut SearchResult<'_, S>) {
        result.highlighted_parts = self.highlighter.highlight(result.sentence);
    }
}

#[derive(Clone, Copy)]
pub struct KeywordHighlighter<'a> {
    keywords: &'a [u32],
}

impl<'a> KeywordHighlighter<'a> {
    pub fn new(keywords: &'a [u32]) -> KeywordHighlighter<'a> {
        KeywordHighlighter { keywords }
    }
}

impl<'a> Highlighter<'a> for KeywordHighlighter<'a> {
    fn highlight<'b, S: Archive>(
        &'a self,
        sentence: &'b ArchivedSentence<S>,
    ) -> Vec<SentenceRange> {
        let mut ranges: Vec<SentenceRange> = Vec::with_capacity(64);

        for keyword in self.keywords {
            let Some(tokens) = sentence.terms_by_value.get(keyword) else {
                continue
            };

            for token_idx in tokens.iter() {
                let token = &sentence.tokens[*token_idx as usize];

                ranges.push(SentenceRange {
                    start: token.start as usize,
                    end: token.end as usize,
                });
            }
        }

        ranges.sort_unstable_by_key(|t| t.start);

        ranges
    }
}

pub struct YokedKeywordsQuery<
    D: DocumentMetadata + 'static,
    S: SentenceMetadata + 'static,
    DF: DocumentFilter<D> + 'static,
> {
    pub inner: Yoke<KeywordsQuery<'static, D, S, DF>, Vec<u32>>,
}

impl<D: DocumentMetadata, S: SentenceMetadata, DF: DocumentFilter<D>> Query<D, S>
    for YokedKeywordsQuery<D, S, DF>
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

unsafe impl<'a, D, S, DF> yoke::Yokeable<'a> for KeywordsQuery<'static, D, S, DF>
where
    D: 'static + DocumentMetadata,
    S: 'static + SentenceMetadata,
    DF: 'static + DocumentFilter<D>,
{
    type Output = KeywordsQuery<'a, D, S, DF>;
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
