use crate::sentence::*;

use rkyv::Archive;

pub trait Highlighter<'a> {
    fn highlight<'b, S: Archive>(&'a self, sentence: &'b ArchivedSentence<S>)
        -> Vec<SentenceRange>;
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
