use std::marker::PhantomData;

use rayon::slice::ParallelSliceMut;
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
pub struct KeywordsQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    pub(crate) keywords: Vec<u32>,
    pub(crate) highlighter: KeywordHighlighter,
    pub(crate) document_filter: DF,
    pub(crate) spooky: PhantomData<(D, S)>,
}

impl<D, S, DF> Query<D, S> for KeywordsQuery<D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>, caller: CallerType) -> SentenceIdList {
        let mut ids = if let [lhs, rhs] = &self.keywords[..] {
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
            for term in &self.keywords {
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

        if DF::needed() && !caller.intersect() {
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

#[derive(Clone)]
pub struct KeywordHighlighter {
    keywords: Vec<u32>,
}

impl KeywordHighlighter {
    pub fn new(keywords: &[u32]) -> KeywordHighlighter {
        KeywordHighlighter {
            keywords: keywords.into(),
        }
    }
}

impl<'a> Highlighter<'a> for KeywordHighlighter {
    fn highlight<'b, S: Archive>(
        &'a self,
        sentence: &'b ArchivedSentence<S>,
    ) -> Vec<SentenceRange> {
        let mut ranges: Vec<SentenceRange> = Vec::with_capacity(64);

        for keyword in &self.keywords {
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
