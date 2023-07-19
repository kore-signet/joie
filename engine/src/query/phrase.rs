use std::marker::PhantomData;

use memchr::memmem::Finder;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use rkyv::Archive;
use storage::Storage;

use crate::{
    highlight::Highlighter,
    id_list::SentenceIdList,
    searcher::{SearchEngine, SearchResult},
    sentence::{ArchivedSentence, SentenceId, SentenceRange},
    DocumentMetadata, SentenceMetadata,
};

use super::{CallerType, DocumentFilter, Query};

#[derive(Clone)]
pub struct PhraseQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    pub(crate) phrase: Vec<u32>,
    pub(crate) highlighter: PhraseHighlighter,
    pub(crate) document_filter: DF,
    pub(crate) spooky: PhantomData<(D, S)>,
}

impl<D, S, DF> Query<D, S> for PhraseQuery<D, S, DF>
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

#[derive(Clone)]
pub struct PhraseHighlighter {
    phrase: Vec<u32>,
    finder: Finder<'static>,
}

impl PhraseHighlighter {
    pub fn new(phrase: &[u32]) -> PhraseHighlighter {
        PhraseHighlighter {
            phrase: Vec::from(phrase),
            finder: Finder::new(bytemuck::cast_slice(phrase)).into_owned(),
        }
    }
}

impl<'a> Highlighter<'a> for PhraseHighlighter {
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
