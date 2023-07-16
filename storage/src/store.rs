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

#[cfg(feature = "persistence")]
pub trait PersistentStorage: Storage {
    type Header: serde::Serialize + serde::de::DeserializeOwned;

    fn header(self) -> Self::Header;

    fn load(header: Self::Header, f: File) -> io::Result<Self>;
}

pub struct SimpleStorage<T: bytemuck::Pod> {
    len: usize,
    store: Mmap,
    spooky: PhantomData<T>,
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

#[cfg(feature = "persistence")]
impl<T: bytemuck::Pod> PersistentStorage for SimpleStorage<T> {
    type Header = usize;

    fn header(self) -> Self::Header {
        self.len
    }

    fn load(header: Self::Header, f: File) -> io::Result<Self> {
        Ok(SimpleStorage {
            len: header,
            store: unsafe { Mmap::map(&f)? },
            spooky: PhantomData,
        })
    }
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

pub struct MultiStorage<T> {
    //  offset, length in terms of T
    positions: Vec<(usize, usize)>,
    store: Mmap,
    spooky: PhantomData<T>,
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

impl<T: bytemuck::Pod> Storage for MultiStorage<T> {
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

#[cfg(feature = "persistence")]
impl<T: bytemuck::Pod> PersistentStorage for MultiStorage<T> {
    type Header = Vec<(usize, usize)>;

    fn header(self) -> Self::Header {
        self.positions
    }

    fn load(header: Self::Header, f: File) -> io::Result<Self> {
        Ok(MultiStorage {
            positions: header,
            store: unsafe { Mmap::map(&f)? },
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

#[cfg(feature = "persistence")]
impl<T: Archive> PersistentStorage for RkyvStorage<T> {
    type Header = Vec<usize>;

    fn header(self) -> Self::Header {
        self.positions
    }

    fn load(header: Self::Header, f: File) -> io::Result<Self> {
        Ok(RkyvStorage {
            positions: header,
            store: unsafe { Mmap::map(&f)? },
            spooky: PhantomData,
        })
    }
}
