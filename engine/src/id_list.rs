use bytemuck::Zeroable;
use rayon::prelude::{IntoParallelRefMutIterator, ParallelIterator};

use crate::sentence::SentenceId;

const PARALLEL_MERGE_THRESH: usize = 32768;

pub struct SentenceIdList {
    pub(crate) ids: Vec<SentenceId>,
}

impl SentenceIdList {
    pub fn from_slice(v: &[SentenceId]) -> SentenceIdList {
        SentenceIdList { ids: v.to_vec() }
    }

    pub fn merge_slices(a: &[SentenceId], b: &[SentenceId]) -> SentenceIdList {
        let mut out = vec![SentenceId::zeroed(); a.len() + b.len()];

        par_merge(a, b, &mut out[..]);

        // // dedup by overwriting every duplicate with the invalid zero value
        // {
        //     let Range {
        //         start: mut cursor,
        //         end,
        //     } = out.as_mut_ptr_range();
        //     cursor = unsafe { cursor.add(1) };
        //     let mut run_start = out.as_mut_ptr();

        //     unsafe {
        //         while cursor != end {
        //             if cursor.read() == run_start.read() {
        //                 cursor.write_bytes(0, 1);
        //             } else {
        //                 run_start = cursor;
        //             }

        //             cursor = cursor.add(1);
        //         }
        //     }
        // }
        // out.retain(|v| v.is_valid());

        // out.dedup();

        SentenceIdList { ids: out }
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

/* the merge zone */

#[inline(always)]
unsafe fn split_unchecked<T>(slice: &[T], k: usize) -> (&[T], &[T]) {
    (
        std::slice::from_raw_parts(slice.as_ptr(), k),
        std::slice::from_raw_parts(slice.as_ptr().add(k), slice.len() - k),
    )
}

#[inline(always)]
unsafe fn split_unchecked_mut<T>(slice: &mut [T], k: usize) -> (&mut [T], &mut [T]) {
    (
        std::slice::from_raw_parts_mut(slice.as_mut_ptr(), k),
        std::slice::from_raw_parts_mut(slice.as_mut_ptr().add(k), slice.len() - k),
    )
}

// adaptation of https://stackoverflow.com/a/64127345
fn par_merge<T: Ord + Copy + Send + Sync>(a: &[T], b: &[T], output: &mut [T]) {
    let (a, b) = if a.len() >= b.len() { (a, b) } else { (b, a) };

    if a.is_empty() {
        return;
    }

    if a.len() < PARALLEL_MERGE_THRESH {
        scalar_merge(a, b, output);
        return;
    }

    let pivot = a.len() / 2;
    let s = match b.binary_search(&a[pivot]) {
        Ok(x) => x,
        Err(x) => x,
    };
    let t = pivot + s;

    let (a_left, a_tail) = unsafe { split_unchecked(a, pivot) };
    let (a_mid, a_right) = unsafe { a_tail.split_first().unwrap_unchecked() };

    let (b_left, b_right) = unsafe { split_unchecked(b, s) };

    let (o_left, o_tail) = unsafe { split_unchecked_mut(output, t) };
    let (o_mid, o_right) = unsafe { o_tail.split_first_mut().unwrap_unchecked() };

    *o_mid = *a_mid;

    rayon::join(
        || par_merge(a_left, b_left, o_left),
        || par_merge(a_right, b_right, o_right),
    );
}

pub fn scalar_merge<T: Ord + Copy>(a: &[T], b: &[T], out: &mut [T]) {
    let mut i = 0;
    let mut j = 0;
    let mut k = 0;

    while i < a.len() && j < b.len() {
        if unsafe { *a.get_unchecked(i) } < unsafe { *b.get_unchecked(j) } {
            unsafe { *out.get_unchecked_mut(k) = *a.get_unchecked(i) };
            k += 1;
            i += 1;
        } else {
            unsafe { *out.get_unchecked_mut(k) = *b.get_unchecked(j) };
            k += 1;
            j += 1;
        }
    }

    let remaining_a = a.len() - i;

    unsafe {
        std::ptr::copy_nonoverlapping(a.as_ptr().add(i), out.as_mut_ptr().add(k), remaining_a);
    }

    k += remaining_a;

    unsafe {
        std::ptr::copy_nonoverlapping(b.as_ptr().add(j), out.as_mut_ptr().add(k), b.len() - j);
    }
}

#[cfg(test)]
mod test {
    use std::iter::repeat_with;

    use crate::{id_list::SentenceIdList, sentence::SentenceId};

    fn gen_list(n: usize) -> Vec<SentenceId> {
        let mut l: Vec<SentenceId> = repeat_with(|| SentenceId {
            doc: fastrand::u32(1..256),
            sentence: fastrand::u32(1..256),
        })
        .take(n)
        .collect();
        l.sort();
        l
    }

    #[test]
    fn test_simple_par_merge() {
        test_merge(gen_list(1000), gen_list(1200));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_par_merge() {
        test_merge(gen_list(100_000), gen_list(100_000));
        test_merge(gen_list(0), gen_list(50_000));
        test_merge(gen_list(50_000), gen_list(0));
        test_merge(gen_list(8000), gen_list(4000));

        for _ in 0..1000 {
            test_merge(
                gen_list(fastrand::usize(0..20_000)),
                gen_list(fastrand::usize(0..20_000)),
            );
        }
    }

    fn test_merge(a: Vec<SentenceId>, b: Vec<SentenceId>) {
        let par_merged = SentenceIdList::merge_slices(&a, &b);

        let normal_merged = {
            let mut c = [a, b].concat();
            c.sort();
            c.dedup();
            c
        };

        assert_eq!(par_merged.into_iter().count(), normal_merged.len());
    }
}
