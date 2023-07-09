use crate::sentence::*;
use memchr::memmem::Finder;

use rkyv::Archive;
use smallvec::SmallVec;

pub struct PhraseHighlighter<'a> {
    phrase: &'a [u32],
    finder: Finder<'a>,
}

impl<'a> PhraseHighlighter<'a> {
    pub fn new(phrase: &'a [u32]) -> PhraseHighlighter<'a> {
        PhraseHighlighter {
            phrase,
            finder: Finder::new(bytemuck::cast_slice(phrase)),
        }
    }

    pub fn highlight<'b, S: Archive>(
        &'a self,
        sentence: &'b ArchivedSentence<S>,
    ) -> Option<SmallVec<[SentencePart<'b>; 8]>> {
        let mut cursor: usize = 0;
        let mut highlights: SmallVec<[SentencePart<'_>; 8]> = SmallVec::new();

        let term_bytes: &[u8] = bytemuck::cast_slice(&sentence.terms);

        for idx in self.finder.find_iter(term_bytes) {
            let idx = idx / 4;
            let start_token = &sentence.tokens[idx];
            let end_token = &sentence.tokens[idx + self.phrase.len() - 1];
            if cursor < start_token.start as usize {
                highlights.push(SentencePart::Normal(
                    &sentence.text[cursor..start_token.start as usize],
                ));
            }

            highlights.push(SentencePart::Highlight(
                &sentence.text[start_token.start as usize..end_token.end as usize],
            ));
            cursor = end_token.end as usize;
        }

        if highlights.is_empty() {
            return None;
        }

        if cursor < sentence.text.len() {
            highlights.push(SentencePart::Normal(&sentence.text[cursor..]));
        }

        Some(highlights)
    }
}
