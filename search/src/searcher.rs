use bytemuck::Pod;

use rkyv::Archive;

use crate::query::Query;
use crate::sentence::*;

use storage::{MultiMap, RkyvMap, SimpleStorage};

pub struct SearchEngine<DocumentMetadata: Pod, SentenceMetadata: Archive> {
    pub(crate) doc_meta: SimpleStorage<DocumentMetadata>,
    pub(crate) sentences: RkyvMap<SentenceId, Sentence<SentenceMetadata>>,
    pub(crate) index: MultiMap<u32, SentenceId>,
}

impl<D: Pod, S: Archive> SearchEngine<D, S> {
    pub fn query<'a>(
        &'a self,
        query: &'a impl Query<D, S>,
    ) -> impl Iterator<Item = SentenceWithHighlights<'a, S>> + 'a {
        let ids = query.find_sentence_ids(self);

        ids.map(|sentence_id| (sentence_id, self.sentences.get(&sentence_id).unwrap()))
            .filter(|(_, sentence)| query.filter_sentence(sentence))
            .filter_map(|(sentence_id, sentence)| {
                let Some(highlights) = query.highlight(sentence) else {
                    return None
                };

                Some(SentenceWithHighlights {
                    id: sentence_id,
                    sentence,
                    parts: highlights,
                })
            })
    }
}
