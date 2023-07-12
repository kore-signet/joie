use bytemuck::Zeroable;
use rayon::prelude::{IntoParallelRefMutIterator, ParallelIterator};

use crate::sentence::SentenceId;

pub struct SentenceIdList {
    pub(crate) ids: Vec<SentenceId>,
}

impl SentenceIdList {
    pub fn from_slice(v: &[SentenceId]) -> SentenceIdList {
        SentenceIdList { ids: v.to_vec() }
    }

    pub fn retain(&mut self, keep: impl Fn(&SentenceId) -> bool + Sync + Send) {
        self.ids.par_iter_mut().for_each(|slot| {
            if slot.is_valid() && !keep(slot) {
                *slot = SentenceId::zeroed();
            }
        })
    }
}

impl IntoIterator for SentenceIdList {
    type Item = SentenceId;

    type IntoIter = SentenceIdListIter;

    fn into_iter(self) -> Self::IntoIter {
        SentenceIdListIter {
            inner: self.ids.into_iter(),
        }
    }
}

pub struct SentenceIdListIter {
    inner: <Vec<SentenceId> as IntoIterator>::IntoIter,
}

impl Iterator for SentenceIdListIter {
    type Item = SentenceId;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .by_ref()
            .find(|&next| next != SentenceId::zeroed())
    }
}
