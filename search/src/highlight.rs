use crate::sentence::*;
use memchr::memmem::Finder;

use rkyv::Archive;
use smallvec::SmallVec;

pub trait Highlighter<'a> {
    fn highlight<'b, S: Archive>(
        &'a self,
        sentence: &'b ArchivedSentence<S>,
    ) -> Option<SmallVec<[SentencePart<'b>; 8]>>;
}

#[derive(Clone)]
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
}

impl<'a> Highlighter<'a> for PhraseHighlighter<'a> {
    fn highlight<'b, S: Archive>(
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

#[derive(Clone, Copy)]
pub struct KeywordHighlighter<'a> {
    keywords: &'a [u32],
}

impl<'a> KeywordHighlighter<'a> {
    pub fn new(keywords: &'a [u32]) -> KeywordHighlighter<'a> {
        KeywordHighlighter { keywords }
    }
}

impl<'a> Highlighter<'a> for KeywordHighlighter<'a> {
    fn highlight<'b, S: Archive>(
        &'a self,
        sentence: &'b ArchivedSentence<S>,
    ) -> Option<SmallVec<[SentencePart<'b>; 8]>> {
        let mut ranges: Vec<Token> = Vec::with_capacity(64);

        for keyword in self.keywords {
            let Some(tokens) = sentence.terms_by_value.get(keyword) else {
                continue
            };

            for token_idx in tokens.iter() {
                let token = &sentence.tokens[*token_idx as usize];

                ranges.push(Token {
                    start: token.start as usize,
                    end: token.end as usize,
                });
            }
        }

        if ranges.is_empty() {
            return None;
        }

        ranges.sort_unstable_by_key(|t| t.start);

        let mut cursor = 0;
        let mut parts: SmallVec<[SentencePart<'b>; 8]> = SmallVec::with_capacity(ranges.len() * 2);

        for Token { start, end } in ranges {
            if start > cursor {
                parts.push(SentencePart::Normal(&sentence.text[cursor..start]));
            }

            parts.push(SentencePart::Highlight(&sentence.text[start..end]));

            cursor = end;
        }

        if cursor < sentence.text.len() {
            parts.push(SentencePart::Normal(&sentence.text[cursor..]));
        }

        Some(parts)
    }
}
