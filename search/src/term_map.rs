use std::collections::{BTreeMap, HashMap};

use perfect_map::PerfectMap;
use rust_stemmers::Stemmer;
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
                        // term,
                    },
                    term,
                )
            })
            .unzip();

        let mut terms_by_value: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        for (idx, term) in terms.iter().enumerate() {
            terms_by_value.entry(*term).or_default().push(idx);
        }

        terms_by_value.values_mut().for_each(|v| v.sort());

        Sentence {
            tokens,
            terms,
            terms_by_value,
            text: s.to_owned(),
            metadata: make_metadata(s),
        }
    }

    pub fn intern(&mut self, term: &str) -> u32 {
        let l = self.kv.len() + 1;
        let term = Stemmer::create(rust_stemmers::Algorithm::English).stem(term);
        *self.kv.entry(term.into()).or_insert(l as u32)
    }

    pub fn freeze(self) -> FrozenTermMap {
        dbg!(self.kv.len());
        FrozenTermMap {
            map: PerfectMap::from_map(self.kv),
        }
    }
    // pub fn tokenize_query(&mut self, query: &str) -> Vec<u32> {
    //     query
    //         .unicode_words()
    //         .map(|word| self.intern(&word.to_lowercase()))
    //         .collect()
    // }
}

pub struct FrozenTermMap {
    map: PerfectMap<CompactString, u32>,
}

impl FrozenTermMap {
    pub fn term(&self, term: &str) -> Option<u32> {
        let term = term.to_lowercase();
        let term = Stemmer::create(rust_stemmers::Algorithm::English).stem(&term);
        self.map.get(term.as_ref()).copied()
    }

    pub fn tokenize_phrase(&self, query: &str) -> Vec<u32> {
        query
            .unicode_words()
            .map(|word| self.term(word).unwrap_or(0u32))
            .collect()
    }
}
