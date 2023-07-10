use crate::sentence::*;
use memchr::memmem::Finder;

use rkyv::Archive;

pub trait Highlighter<'a> {
    fn highlight<'b, S: Archive>(&'a self, sentence: &'b ArchivedSentence<S>)
        -> Vec<SentenceRange>;
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
    ) -> Vec<SentenceRange> {
        let mut ranges: Vec<SentenceRange> = Vec::with_capacity(64);

        for keyword in self.keywords {
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

#[inline(always)]
pub fn collapse_overlapped_ranges(ranges: &[SentenceRange]) -> Vec<SentenceRange> {
    let mut result = Vec::with_capacity(ranges.len());
    let mut ranges_it = ranges.iter();

    let mut current = match ranges_it.next() {
        Some(range) => *range,
        None => return result,
    };

    for range in ranges {
        if current.end > range.start {
            current = SentenceRange {
                start: current.start,
                end: std::cmp::max(current.end, range.end),
            };
        } else {
            result.push(current);
            current = *range;
        }
    }

    result.push(current);
    result
}

pub fn highlight_by_ranges<'a>(ranges: &[SentenceRange], text: &'a str) -> Vec<SentencePart<'a>> {
    let mut cursor = 0;
    let mut results = Vec::with_capacity(ranges.len() * 2);

    for SentenceRange { start, end } in ranges.iter().copied() {
        if cursor < start {
            results.push(SentencePart::Normal(&text[cursor..start]));
        }

        results.push(SentencePart::Highlight(&text[start..end]));

        cursor = end;
    }

    if cursor < text.len() {
        results.push(SentencePart::Normal(&text[cursor..]));
    }

    results
}
