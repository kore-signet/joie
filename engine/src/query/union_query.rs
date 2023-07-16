use std::marker::PhantomData;

use arrayvec::ArrayVec;
use rayon::{
    prelude::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSliceMut,
};

use crate::{
    highlight::collapse_overlapped_ranges,
    id_list::SentenceIdList,
    searcher::{SearchEngine, SearchResult},
    sentence::SentenceId,
    DocumentMetadata, SentenceMetadata,
};

use super::{CallerType, Query};

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

    pub fn from_boxed(
        queries: impl IntoIterator<Item = Box<dyn Query<D, S> + Send + Sync + 'a>>,
    ) -> UnionQuery<'a, D, S> {
        UnionQuery {
            queries: ArrayVec::from_iter(queries.into_iter()),
            spooky: PhantomData,
        }
    }
}

impl<'q, D: DocumentMetadata, S: SentenceMetadata> Query<D, S> for UnionQuery<'q, D, S> {
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
