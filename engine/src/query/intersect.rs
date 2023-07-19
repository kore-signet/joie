use rayon::{
    prelude::{IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSliceMut,
};
use smallvec::SmallVec;
use std::marker::PhantomData;
use storage::Storage;

use crate::{
    highlight::collapse_overlapped_ranges,
    id_list::SentenceIdList,
    searcher::{SearchEngine, SearchResult},
    sentence::SentenceId,
    DocumentMetadata, SentenceMetadata,
};

use super::{CallerType, DocumentFilter, DynamicQuery, PhraseQuery, Query};

#[derive(Default)]
pub struct IntersectingQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    queries: SmallVec<[Box<DynamicQuery<D, S, DF>>; 4]>,
    document_filter: DF,
    spooky: PhantomData<(D, S)>,
}

impl<D, S, DF> IntersectingQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    pub fn and(&mut self, query: impl Into<DynamicQuery<D, S, DF>>) {
        self.queries.push(Box::new(query.into()))
    }

    pub fn from_boxed(
        queries: impl IntoIterator<Item = impl Into<DynamicQuery<D, S, DF>>>,
        document_filter: DF,
    ) -> IntersectingQuery<D, S, DF> {
        IntersectingQuery {
            queries: SmallVec::from_iter(queries.into_iter().map(|v| v.into()).map(Box::new)),
            document_filter,
            spooky: PhantomData,
        }
    }
}

impl<D, S, DF> Query<D, S> for IntersectingQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>, _caller: CallerType) -> SentenceIdList {
        let mut sets: Vec<SentenceIdList> = self
            .queries
            .par_iter()
            .map(|v| v.find_sentence_ids(db, CallerType::Intersection))
            .collect();
        sets.sort_by_key(|v| v.ids.len());

        if sets.len() > 1 {
            let (lhs, rhs) = sets.split_at_mut(1);
            let res = &mut lhs[0];

            res.ids.par_sort_unstable();
            for set in rhs {
                set.ids.par_sort_unstable();
                res.retain(|v| {
                    set.ids.binary_search(v).is_ok()
                        && self
                            .document_filter
                            .filter_document(unsafe { db.doc_meta.get_unchecked(v.doc as usize) })
                })
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
        let mut highlights = std::mem::take(&mut result.highlighted_parts);
        for query in &self.queries {
            query.find_highlights(result);

            highlights.append(&mut result.highlighted_parts);
        }

        highlights.par_sort_by_key(|v| v.start);
        result.highlighted_parts = collapse_overlapped_ranges(&highlights);
    }
}

#[derive(Default)]
pub struct IntersectingPhraseQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    queries: SmallVec<[PhraseQuery<D, S, DF>; 2]>,
    document_filter: DF,
    spooky: PhantomData<(D, S)>,
}

impl<D, S, DF> IntersectingPhraseQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    pub fn from_iter(
        queries: impl IntoIterator<Item = PhraseQuery<D, S, DF>>,
        filter: DF,
    ) -> IntersectingPhraseQuery<D, S, DF> {
        IntersectingPhraseQuery {
            document_filter: filter,
            queries: SmallVec::from_iter(queries.into_iter()),
            spooky: PhantomData,
        }
    }
}

impl<D, S, DF> Query<D, S> for IntersectingPhraseQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>, _caller: CallerType) -> SentenceIdList {
        let mut term_sets: Vec<&[SentenceId]> = self
            .queries
            .par_iter()
            .flat_map(|query| {
                query
                    .phrase
                    .par_iter()
                    .map(|term| db.index.get(term).unwrap_or(&[]))
            })
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
        let mut highlights = std::mem::take(&mut result.highlighted_parts);
        for query in &self.queries {
            query.find_highlights(result);

            highlights.append(&mut result.highlighted_parts);
        }

        highlights.par_sort_by_key(|v| v.start);
        result.highlighted_parts = collapse_overlapped_ranges(&highlights);
    }
}
