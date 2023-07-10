use bytemuck::Pod;

use rkyv::Archive;

use crate::{highlight::highlight_by_ranges, query::Query};
use crate::{sentence::*, CopyableRange};

use storage::{MultiMap, RkyvMap, SimpleStorage};

pub struct SearchEngine<DocumentMetadata: Pod, SentenceMetadata: Archive> {
    pub(crate) doc_meta: SimpleStorage<DocumentMetadata>,
    pub(crate) sentences: RkyvMap<SentenceId, Sentence<SentenceMetadata>>,
    pub(crate) index: MultiMap<u32, SentenceId>,
}

#[derive(Clone)]
pub struct SearchResult<'a, M: Archive> {
    pub id: SentenceId,
    pub highlighted_parts: Vec<CopyableRange>,
    pub sentence: &'a ArchivedSentence<M>,
}

impl<'a, M: Archive> SearchResult<'a, M> {
    pub fn highlights(&'a self) -> Vec<SentencePart> {
        highlight_by_ranges(&self.highlighted_parts, &self.sentence.text)
    }
}

impl<D: Pod, S: Archive> SearchEngine<D, S> {
    pub fn query<'a>(
        &'a self,
        query: &'a impl Query<D, S>,
    ) -> impl Iterator<Item = SearchResult<'a, S>> + 'a {
        let ids = query.find_sentence_ids(self);

        ids.map(|sentence_id| SearchResult {
            id: sentence_id,
            highlighted_parts: Vec::new(),
            sentence: self.sentences.get(&sentence_id).unwrap(),
        })
        .filter_map(|r| query.filter_map(r))
    }
}
