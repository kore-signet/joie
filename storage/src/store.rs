use std::{fs::File, marker::PhantomData};

use memmap2::{Mmap, MmapOptions};

use rkyv::ser::serializers::AllocSerializer;
use rkyv::Archive;
use zerocopy::AsBytes;

pub trait Storage: Sized {
    type Item<'a>
    where
        Self: 'a;

    fn get(&self, idx: usize) -> Self::Item<'_> {
        self.try_get(idx).unwrap()
    }

    fn try_get(&self, idx: usize) -> Option<Self::Item<'_>>;

    unsafe fn get_unchecked(&self, idx: usize) -> Self::Item<'_>;
}

pub struct SimpleStorage<T: bytemuck::Pod> {
    len: usize,
    store: Mmap,
    spooky: PhantomData<T>,
}

impl<T: bytemuck::Pod> SimpleStorage<T> {
    pub fn build(values: &[T], file: File) -> std::io::Result<SimpleStorage<T>> {
        let bytes: &[u8] = bytemuck::cast_slice(values);
        file.set_len(bytes.len() as u64)?;
        let mut map = unsafe { MmapOptions::new().map_mut(&file)? };
        map.copy_from_slice(bytes);

        Ok(SimpleStorage {
            len: values.len(),
            store: map.make_read_only()?,
            spooky: PhantomData,
        })
    }
}

impl<T: bytemuck::Pod> Storage for SimpleStorage<T> {
    type Item<'a> = &'a T;

    fn try_get(&self, idx: usize) -> Option<Self::Item<'_>> {
        if idx < self.len {
            Some(unsafe { self.get_unchecked(idx) })
        } else {
            None
        }
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::Item<'_> {
        let ptr: *const T = (self.store.as_ptr() as *const T).add(idx);

        &*ptr
    }
}

pub struct MultiStorage<T> {
    //  offset, length in terms of T
    positions: Vec<(usize, usize)>,
    store: Mmap,
    spooky: PhantomData<T>,
}

impl<T: bytemuck::Pod> MultiStorage<T> {
    pub fn build(values: &[impl AsRef<[T]>], file: File) -> std::io::Result<MultiStorage<T>> {
        file.set_len(
            values
                .iter()
                .map(|v| std::mem::size_of_val(v.as_ref()))
                .sum::<usize>() as u64,
        )?;
        let mut map = unsafe { MmapOptions::new().populate().map_mut(&file)? };

        let mut positions = Vec::with_capacity(values.len());

        let mut pos: usize = 0;

        for value in values {
            let value = value.as_ref();
            positions.push((pos, value.len()));

            let out_slice = unsafe {
                std::slice::from_raw_parts_mut((map.as_mut_ptr() as *mut T).add(pos), value.len())
            };

            out_slice.copy_from_slice(value);

            pos += value.len();
        }

        Ok(MultiStorage {
            positions,
            store: map.make_read_only()?,
            spooky: PhantomData,
        })
    }
}

impl<T> Storage for MultiStorage<T> {
    type Item<'a> = &'a [T] where Self: 'a;

    fn try_get(&self, idx: usize) -> Option<Self::Item<'_>> {
        self.positions.get(idx).map(|(pos, len)| unsafe {
            std::slice::from_raw_parts((self.store.as_ptr() as *const T).add(*pos), *len)
        })
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::Item<'_> {
        let (pos, len) = self.positions.get_unchecked(idx);
        std::slice::from_raw_parts((self.store.as_ptr() as *const T).add(*pos), *len)
    }
}

pub struct RkyvStorage<T> {
    //  offset in terms of bytes
    store: Mmap,
    spooky: PhantomData<T>,
}

impl<T: rkyv::Serialize<AllocSerializer<1024>>> RkyvStorage<T> {
    pub fn build(values: Vec<T>, file: File) -> std::io::Result<RkyvStorage<T>> {
        let serialized_values = rkyv::to_bytes::<_, 1024>(&values).unwrap();
        file.set_len(serialized_values.len() as u64)?;
        let mut map = unsafe { MmapOptions::new().populate().map_mut(&file)? };
        map.copy_from_slice(serialized_values.as_bytes());

        Ok(RkyvStorage {
            store: map.make_read_only()?,
            spooky: PhantomData,
        })
    }
}

impl<T: Archive> Storage for RkyvStorage<T> {
    type Item<'a> = &'a T::Archived where Self: 'a;

    fn try_get(&self, idx: usize) -> Option<Self::Item<'_>> {
        let vals = unsafe { rkyv::archived_root::<Vec<T>>(&self.store) };
        vals.get(idx)
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::Item<'_> {
        let vals = unsafe { rkyv::archived_root::<Vec<T>>(&self.store) };
        vals.get_unchecked(idx)
    }
}
