use highlight::PhraseHighlighter;

use rkyv::Archive;
use stable_vec::StableVec;

pub mod highlight;
pub mod sentence;
pub mod term_map;
use sentence::*;

use storage::{MultiMap, RkyvMap, SimpleStorage, Storage};

pub struct Db<DocumentMetadata: bytemuck::Pod, SentenceMetadata: Archive> {
    pub doc_meta: SimpleStorage<DocumentMetadata>,
    pub sentences: RkyvMap<SentenceId, Sentence<SentenceMetadata>>,
    pub index: MultiMap<u32, SentenceId>,
}

impl<D: bytemuck::Pod, S: Archive> Db<D, S> {
    fn ids_for_phrase<'a>(
        &'a self,
        phrase: &'a [u32],
        document_filter: Option<impl Fn(&'a D) -> bool>,
    ) -> StableVec<&'a SentenceId> {
        let mut term_sets = Vec::with_capacity(phrase.len());
        for term in phrase {
            let ids = self.index.get(term).unwrap_or(&[]);
            term_sets.push(ids);
        }

        term_sets.sort_by_key(|v| v.len());

        let mut sentence_ids = StableVec::from_iter(term_sets[0].iter());

        if term_sets.len() > 1 {
            if let Some(filter) = document_filter {
                for set in &term_sets[1..] {
                    sentence_ids.retain(|v| {
                        set.binary_search(v).is_ok()
                            && filter(unsafe { self.doc_meta.get_unchecked(v.doc as usize) })
                    });
                }
            } else {
                for set in &term_sets[1..] {
                    sentence_ids.retain(|v| set.binary_search(v).is_ok());
                }
            };
        } else if let Some(filter) = document_filter {
            sentence_ids
                .retain(|id| filter(unsafe { self.doc_meta.get_unchecked(id.doc as usize) }));
        }

        sentence_ids
    }

    pub fn query_phrase<'a>(
        &'a self,
        phrase: &'a [u32],
        document_filter: Option<impl Fn(&'a D) -> bool>,
        sentence_filter: impl Fn(&'a S::Archived) -> bool + 'a,
    ) -> impl Iterator<Item = SentenceWithHighlights<'a, S>> + 'a {
        let highlighter = PhraseHighlighter::new(phrase);
        self.ids_for_phrase(phrase, document_filter)
            .into_iter()
            .map(|(_, sentence_id)| (*sentence_id, self.sentences.get(sentence_id).unwrap()))
            .filter(move |(_, sentence)| sentence_filter(&sentence.metadata))
            .filter_map(move |(sentence_id, sentence)| {
                let Some(highlights) = highlighter.highlight(sentence) else {
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
