use std::{marker::PhantomData, ops::Deref};

use arrayvec::ArrayVec;

use rayon::prelude::*;
use rkyv::Archive;

use storage::Storage;
use yoke::{Yoke, Yokeable};

use crate::{
    highlight::{collapse_overlapped_ranges, Highlighter, KeywordHighlighter, PhraseHighlighter},
    id_list::SentenceIdList,
    searcher::{SearchEngine, SearchResult},
    sentence::{ArchivedSentence, SentenceId},
    DocumentMetadata, SentenceMetadata,
};

#[inline(always)]
pub const fn always_true<S: Archive>(_sentence: &ArchivedSentence<S>) -> bool {
    true
}

pub trait SentenceFilter<S: SentenceMetadata>: Send + Sync {
    fn filter_sentence(&self, sentence: &ArchivedSentence<S>) -> bool;
}

pub trait DocumentFilter<D: DocumentMetadata>: Send + Sync {
    fn filter_document(&self, document_meta: &D) -> bool;

    fn needed() -> bool {
        true
    }
}

impl<T, D: DocumentMetadata> DocumentFilter<D> for T
where
    T: Fn(&D) -> bool + Send + Sync,
{
    #[inline(always)]
    fn filter_document(&self, document_meta: &D) -> bool {
        self(document_meta)
    }
}

impl<D: DocumentMetadata> DocumentFilter<D> for () {
    #[inline(always)]
    fn filter_document(&self, _document_meta: &D) -> bool {
        true
    }

    fn needed() -> bool {
        false
    }
}

impl<T, S: SentenceMetadata> SentenceFilter<S> for T
where
    T: Fn(&ArchivedSentence<S>) -> bool + Send + Sync,
{
    #[inline(always)]
    fn filter_sentence(&self, sentence: &ArchivedSentence<S>) -> bool {
        self(sentence)
    }
}

pub trait Query<D: DocumentMetadata, S: SentenceMetadata> {
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>) -> SentenceIdList;

    fn filter_map(&self, _result: &mut SearchResult<'_, S>) -> bool {
        true
    }

    fn find_highlights(&self, sentence: &mut SearchResult<'_, S>);
}

#[derive(Clone, Copy)]
pub struct QueryBuilder<'a, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    terms: &'a [u32],
    document_filter: DF,
    spooky: PhantomData<(D, S)>,
}

impl<'a, D: DocumentMetadata, S: SentenceMetadata> QueryBuilder<'a, D, S, ()> {
    pub fn start(phrase: &'a [u32]) -> QueryBuilder<'a, D, S, ()> {
        QueryBuilder {
            terms: phrase,
            document_filter: (),
            spooky: PhantomData,
        }
    }
}

impl<'a, D, S, DF> QueryBuilder<'a, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    pub fn filter_documents<NDF: DocumentFilter<D>>(
        self,
        doc_filter: NDF,
    ) -> QueryBuilder<'a, D, S, NDF> {
        QueryBuilder {
            terms: self.terms,
            document_filter: doc_filter,
            spooky: PhantomData,
        }
    }

    pub fn phrases(self) -> PhraseQuery<'a, D, S, DF> {
        PhraseQuery {
            phrase: self.terms,
            highlighter: PhraseHighlighter::new(self.terms),
            document_filter: self.document_filter,
            spooky: PhantomData,
        }
    }

    pub fn keywords(self) -> KeywordsQuery<'a, D, S, DF> {
        KeywordsQuery {
            keywords: self.terms,
            highlighter: KeywordHighlighter::new(self.terms),
            document_filter: self.document_filter,
            spooky: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct PhraseQuery<'a, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    phrase: &'a [u32],
    highlighter: PhraseHighlighter<'a>,
    document_filter: DF,
    spooky: PhantomData<(D, S)>,
}

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

impl<'b, D, S, DF> Query<D, S> for PhraseQuery<'b, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D> + Sync + Send,
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>) -> SentenceIdList {
        let mut term_sets: Vec<&[SentenceId]> = self
            .phrase
            .par_iter()
            .map(|term| db.index.get(term).unwrap_or(&[]))
            .collect();

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

#[derive(Clone, Copy)]
pub struct KeywordsQuery<'a, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    keywords: &'a [u32],
    highlighter: KeywordHighlighter<'a>,
    document_filter: DF,
    spooky: PhantomData<(D, S)>,
}

impl<'b, D, S, DF> Query<D, S> for KeywordsQuery<'b, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>) -> SentenceIdList {
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

        let mut ids = SentenceIdList { ids };

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

#[derive(Default)]
pub struct IntersectingQuery<'a, D, S>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
{
    queries: ArrayVec<Box<(dyn Query<D, S> + Send + Sync + 'a)>, 4>,
    spooky: PhantomData<(D, S)>,
}

impl<'a, D, S> IntersectingQuery<'a, D, S>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
{
    pub fn and(&mut self, query: impl Query<D, S> + Send + Sync + 'a) {
        self.queries.push(Box::new(query))
    }

    pub fn and_boxed(&mut self, query: Box<dyn Query<D, S> + Send + Sync + 'a>) {
        self.queries.push(query)
    }
}

impl<'q, D, S> Query<D, S> for IntersectingQuery<'q, D, S>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>) -> SentenceIdList {
        let mut sets: Vec<SentenceIdList> = self
            .queries
            .par_iter()
            .map(|v| v.find_sentence_ids(db))
            .collect();
        sets.sort_by_key(|v| v.ids.len());

        if sets.len() > 1 {
            let (lhs, rhs) = sets.split_at_mut(1);
            let res = &mut lhs[0];

            res.ids.par_sort_unstable();
            for set in rhs {
                set.ids.par_sort_unstable();
                res.retain(|v| set.ids.binary_search(v).is_ok())
            }
        }

        sets.swap_remove(0)
    }

    fn filter_map(&self, result: &mut SearchResult<'_, S>) -> bool {
        let mut highlights = Vec::new();
        for query in &self.queries {
            if !query.filter_map(result) {
                return false;
            }

            highlights.append(&mut result.highlighted_parts);
        }

        if highlights.is_empty() {
            return false;
        }

        highlights.par_sort_by_key(|v| v.start);
        result.highlighted_parts = collapse_overlapped_ranges(&highlights);

        true
    }

    fn find_highlights(&self, result: &mut SearchResult<'_, S>) {
        let mut highlights = Vec::new();
        for query in &self.queries {
            query.find_highlights(result);

            highlights.append(&mut result.highlighted_parts);
        }

        highlights.par_sort_by_key(|v| v.start);
        result.highlighted_parts = collapse_overlapped_ranges(&highlights);
    }
}

#[derive(Default)]
pub struct UnionQuery<'a, D, S>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
{
    queries: ArrayVec<Box<(dyn Query<D, S> + Send + Sync + 'a)>, 4>,
    spooky: PhantomData<(D, S)>,
}

impl<'a, D: DocumentMetadata, S: SentenceMetadata> UnionQuery<'a, D, S> {
    pub fn or(&mut self, query: (impl Query<D, S> + Send + Sync + 'a)) {
        self.queries.push(Box::new(query))
    }

    pub fn or_boxed(&mut self, query: Box<dyn Query<D, S> + Send + Sync + 'a>) {
        self.queries.push(query)
    }
}

impl<'q, D: DocumentMetadata, S: SentenceMetadata> Query<D, S> for UnionQuery<'q, D, S> {
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>) -> SentenceIdList {
        let mut sets: Vec<SentenceId> = self
            .queries
            .par_iter()
            .flat_map(|v| {
                v.find_sentence_ids(db)
                    .ids
                    .into_par_iter()
                    .filter(SentenceId::is_valid)
            })
            .collect();

        sets.par_sort_unstable();
        sets.dedup();

        SentenceIdList { ids: sets }
    }

    fn filter_map(&self, result: &mut SearchResult<'_, S>) -> bool {
        let mut highlights = Vec::new();

        for query in &self.queries {
            query.filter_map(result);

            highlights.append(&mut result.highlighted_parts);
        }

        if highlights.is_empty() {
            return false;
        }

        highlights.par_sort_by_key(|v| v.start);
        result.highlighted_parts = collapse_overlapped_ranges(&highlights);

        true
    }

    fn find_highlights(&self, result: &mut SearchResult<'_, S>) {
        let mut highlights = Vec::new();
        for query in &self.queries {
            query.find_highlights(result);

            highlights.append(&mut result.highlighted_parts);
        }

        highlights.par_sort_by_key(|v| v.start);
        result.highlighted_parts = collapse_overlapped_ranges(&highlights);
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
    ) -> crate::id_list::SentenceIdList {
        self.inner.get().find_sentence_ids(db)
    }

    fn find_highlights(&self, sentence: &mut crate::searcher::SearchResult<'_, S>) {
        self.inner.get().find_highlights(sentence)
    }

    fn filter_map(&self, result: &mut crate::searcher::SearchResult<'_, S>) -> bool {
        self.inner.get().filter_map(result)
    }
}

pub struct YokedDynQuery<D: DocumentMetadata + 'static, S: SentenceMetadata + 'static> {
    pub inner: Yoke<DynQuery<'static, D, S>, Vec<u32>>,
}

impl<D: DocumentMetadata + 'static, S: SentenceMetadata + 'static> Query<D, S>
    for YokedDynQuery<D, S>
{
    fn find_sentence_ids(
        &self,
        db: &crate::searcher::SearchEngine<D, S>,
    ) -> crate::id_list::SentenceIdList {
        self.inner.get().find_sentence_ids(db)
    }

    fn find_highlights(&self, sentence: &mut crate::searcher::SearchResult<'_, S>) {
        self.inner.get().find_highlights(sentence)
    }

    fn filter_map(&self, result: &mut crate::searcher::SearchResult<'_, S>) -> bool {
        self.inner.get().filter_map(result)
    }
}

#[derive(Yokeable)]
#[repr(transparent)]
pub struct DynQuery<'a, D: DocumentMetadata, S: SentenceMetadata> {
    pub(crate) inner: Box<dyn Query<D, S> + Send + Sync + 'a>,
}

impl<'a, D: DocumentMetadata + 'a, S: SentenceMetadata + 'a> Query<D, S> for DynQuery<'a, D, S> {
    fn find_sentence_ids(
        &self,
        db: &crate::searcher::SearchEngine<D, S>,
    ) -> crate::id_list::SentenceIdList {
        self.inner.find_sentence_ids(db)
    }

    fn find_highlights(&self, sentence: &mut crate::searcher::SearchResult<'_, S>) {
        self.inner.find_highlights(sentence)
    }

    fn filter_map(&self, result: &mut crate::searcher::SearchResult<'_, S>) -> bool {
        self.inner.filter_map(result)
    }
}

impl<'a, D: DocumentMetadata, S: SentenceMetadata> Deref for DynQuery<'a, D, S> {
    type Target = dyn Query<D, S> + Send + Sync + 'a;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}
