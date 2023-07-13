use std::collections::BTreeMap;

use bytemuck::{Pod, Zeroable};

use rkyv::Archive;
use smallvec::SmallVec;

use crate::{CopyableRange, Token};
pub type SentenceRange = CopyableRange;

pub struct SentenceWithHighlights<'a, M: Archive> {
    pub id: SentenceId,
    pub sentence: &'a ArchivedSentence<M>,
    pub parts: Vec<SentencePart<'a>>,
}

#[derive(Pod, Clone, Copy, Zeroable, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(C, align(8))]
pub struct SentenceId {
    pub doc: u32,
    pub sentence: u32,
}

impl SentenceId {
    pub fn new(doc: u32, sentence: u32) -> SentenceId {
        SentenceId { doc, sentence }
    }

    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        *self != SentenceId::zeroed()
    }
}

#[derive(Clone, Archive, rkyv::Serialize)]
pub struct Sentence<M> {
    pub text: String,
    pub tokens: Vec<Token>,
    // (u32 term -> idx in tokens array)
    pub terms_by_value: BTreeMap<u32, SmallVec<[usize; 4]>>,
    pub terms: Vec<u32>,
    pub metadata: M,
}

pub enum SentencePart<'a> {
    Normal(&'a str),
    Highlight(&'a str),
}
