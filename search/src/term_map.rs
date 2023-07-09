use std::collections::HashMap;

use smartstring::alias::CompactString;
use unicode_segmentation::UnicodeSegmentation;

use crate::sentence::{Sentence, Token};

#[derive(Default, Clone, Debug)]
pub struct TermMap {
    pub kv: HashMap<CompactString, u32>,
}

impl TermMap {
    pub fn tokenize_all<M>(
        &mut self,
        doc: &str,
        make_metadata: impl Fn(&str) -> M + Clone,
    ) -> Vec<Sentence<M>> {
        doc.lines()
            .map(|s| self.tokenize_sentence(s, make_metadata.clone()))
            .collect()
    }

    pub fn tokenize_sentence<M>(
        &mut self,
        s: &str,
        make_metadata: impl Fn(&str) -> M,
    ) -> Sentence<M> {
        let (tokens, terms): (Vec<Token>, Vec<u32>) = s
            .unicode_word_indices()
            .map(|(start, word)| {
                let lower = word.to_lowercase();
                let term = self.intern(&lower);

                (
                    Token {
                        start,
                        end: start + word.len(),
                        term,
                    },
                    term,
                )
            })
            .unzip();

        let mut terms_by_value = terms.iter().copied().enumerate().collect::<Vec<_>>();
        terms_by_value.sort_by(|(_, lhs), (_, rhs)| lhs.cmp(rhs));

        Sentence {
            tokens,
            terms,
            terms_by_value,
            text: s.to_owned(),
            metadata: make_metadata(s),
        }
    }

    pub fn intern(&mut self, term: &str) -> u32 {
        let l = self.kv.len();
        *self.kv.entry(term.into()).or_insert(l as u32)
    }

    pub fn tokenize_query(&mut self, query: &str) -> Vec<u32> {
        query
            .unicode_words()
            .map(|word| self.intern(&word.to_lowercase()))
            .collect()
    }
}
