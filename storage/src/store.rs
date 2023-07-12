use std::io::{self, BufWriter, Seek, Write};
use std::{fs::File, marker::PhantomData};

use memmap2::{Mmap, MmapOptions};

use rkyv::ser::serializers::{
    AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer,
};
use rkyv::ser::Serializer;
use rkyv::Archive;

use crate::SerializableToFile;

macro_rules! try_serializer {
    ($e:expr) => {
        $e.map_err(|e| match e {
            rkyv::ser::serializers::CompositeSerializerError::SerializerError(e) => e,
            _ => unreachable!()
        })?
    };
}

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

pub struct MultiStorageBuilder<T: bytemuck::Pod> {
    cursor: usize,
    positions: Vec<(usize, usize)>,
    out: BufWriter<File>,
    spooky: PhantomData<T>,
}

impl<T: bytemuck::Pod> MultiStorageBuilder<T> {
    pub fn new(length: usize, file: File) -> MultiStorageBuilder<T> {
        MultiStorageBuilder {
            cursor: 0,
            positions: vec![(0, 0); length],
            out: BufWriter::new(file),
            spooky: PhantomData,
        }
    }

    pub fn serialize(&mut self, index: usize, value: &[T]) -> io::Result<()> {
        let start = self.cursor;

        self.out.write_all(bytemuck::cast_slice(value))?;

        assert!(self.out.stream_position()? as usize % std::mem::align_of::<T>() == 0);

        self.positions[index] = (start, value.len());

        self.cursor += value.len();

        Ok(())
    }

    pub fn finish(mut self) -> io::Result<MultiStorage<T>> {
        self.out.flush()?;
        let (file, _) = self.out.into_parts();

        let map = unsafe { MmapOptions::new().populate().map(&file)? };

        Ok(MultiStorage {
            positions: self.positions,
            store: map,
            spooky: PhantomData,
        })
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

pub struct RkyvStorageBuilder<T: SerializableToFile> {
    serializer:
        CompositeSerializer<WriteSerializer<BufWriter<File>>, AllocScratch, SharedSerializeMap>,
    positions: Vec<usize>,
    spooky: PhantomData<T>,
}

impl<T: SerializableToFile> RkyvStorageBuilder<T> {
    pub fn create(length: usize, file: File) -> io::Result<RkyvStorageBuilder<T>> {
        let positions = vec![0; length];
        let mut serializer: CompositeSerializer<
            WriteSerializer<BufWriter<File>>,
            AllocScratch,
            SharedSerializeMap,
        > = CompositeSerializer::new(
            WriteSerializer::new(BufWriter::new(file)),
            AllocScratch::new(),
            SharedSerializeMap::new(),
        );
        try_serializer!(serializer.align_for::<T>());

        Ok(RkyvStorageBuilder {
            serializer,
            positions,
            spooky: PhantomData,
        })
    }

    pub fn serialize(&mut self, index: usize, data: &T) -> io::Result<()> {
        self.positions[index] = try_serializer!(self.serializer.serialize_value(data));

        Ok(())
    }

    pub fn finish(self) -> io::Result<RkyvStorage<T>> {
        let mut writer = self.serializer.into_serializer().into_inner();
        writer.flush()?;
        let (file, _) = writer.into_parts();
        // let serialized_values = rkyv::to_bytes::<_, 1024>(&values).unwrap();
        // file.set_len(serialized_values.len() as u64)?;
        let map = unsafe { MmapOptions::new().populate().map_mut(&file)? };
        // map.copy_from_slice(serialized_values.as_bytes());

        Ok(RkyvStorage {
            positions: self.positions,
            store: map.make_read_only()?,
            spooky: PhantomData,
        })
    }
}

pub struct RkyvStorage<T> {
    //  offset in terms of bytes
    positions: Vec<usize>,
    store: Mmap,
    spooky: PhantomData<T>,
}

impl<T: SerializableToFile> RkyvStorage<T> {
    pub fn build(values: Vec<T>, file: File) -> std::io::Result<RkyvStorage<T>> {
        let mut serializer: CompositeSerializer<
            WriteSerializer<BufWriter<File>>,
            AllocScratch,
            SharedSerializeMap,
        > = CompositeSerializer::new(
            WriteSerializer::new(BufWriter::new(file)),
            AllocScratch::new(),
            SharedSerializeMap::new(),
        );
        try_serializer!(serializer.align_for::<T>());

        let mut positions = Vec::with_capacity(values.len());

        for value in values {
            positions.push(try_serializer!(serializer.serialize_value(&value)));
        }

        let mut writer = serializer.into_serializer().into_inner();
        writer.flush()?;
        let (file, _) = writer.into_parts();
        // let serialized_values = rkyv::to_bytes::<_, 1024>(&values).unwrap();
        // file.set_len(serialized_values.len() as u64)?;
        let map = unsafe { MmapOptions::new().populate().map_mut(&file)? };
        // map.copy_from_slice(serialized_values.as_bytes());

        Ok(RkyvStorage {
            positions,
            store: map.make_read_only()?,
            spooky: PhantomData,
        })
    }
}

impl<T: Archive> Storage for RkyvStorage<T> {
    type Item<'a> = &'a T::Archived where Self: 'a;

    fn try_get(&self, idx: usize) -> Option<Self::Item<'_>> {
        self.positions
            .get(idx)
            .map(|pos| unsafe { rkyv::archived_value::<T>(&self.store, *pos) })
    }

    unsafe fn get_unchecked(&self, idx: usize) -> Self::Item<'_> {
        unsafe { rkyv::archived_value::<T>(&self.store, *self.positions.get_unchecked(idx)) }
    }
}
