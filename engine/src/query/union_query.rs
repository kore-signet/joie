use std::marker::PhantomData;

use rayon::{
    prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSliceMut,
};
use smallvec::SmallVec;

use crate::{
    highlight::collapse_overlapped_ranges,
    id_list::SentenceIdList,
    searcher::{SearchEngine, SearchResult},
    sentence::SentenceId,
    DocumentMetadata, SentenceMetadata,
};

use super::{CallerType, DocumentFilter, DynamicQuery, Query};

#[derive(Default)]
pub struct UnionQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    queries: SmallVec<[Box<DynamicQuery<D, S, DF>>; 4]>,
    spooky: PhantomData<(D, S)>,
}

impl<D: DocumentMetadata, S: SentenceMetadata, DF: DocumentFilter<D>> UnionQuery<D, S, DF> {
    pub fn or(&mut self, query: impl Into<DynamicQuery<D, S, DF>>) {
        self.queries.push(Box::new(query.into()))
    }

    pub fn from_dynamic(
        queries: impl IntoIterator<Item = impl Into<DynamicQuery<D, S, DF>>>,
    ) -> UnionQuery<D, S, DF> {
        UnionQuery {
            queries: SmallVec::from_iter(queries.into_iter().map(|v| v.into()).map(Box::new)),
            spooky: PhantomData,
        }
    }
}

impl<D: DocumentMetadata, S: SentenceMetadata, DF: DocumentFilter<D>> Query<D, S>
    for UnionQuery<D, S, DF>
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>, _caller: CallerType) -> SentenceIdList {
        let mut sets: Vec<SentenceId> = self
            .queries
            .par_iter()
            .flat_map(|v| {
                v.find_sentence_ids(db, CallerType::Union)
                    .ids
                    .into_par_iter()
                    .filter(SentenceId::is_valid)
            })
            .collect();

        sets.par_sort();
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
