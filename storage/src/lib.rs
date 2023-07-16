use std::collections::HashMap;
use std::hash::Hash;
#[cfg(feature = "persistence")]
use std::io::ErrorKind;
use std::io::{self, BufWriter};
use std::{fmt::Debug, fs::File};

pub mod store;

#[cfg(feature = "persistence")]
use serde::de::DeserializeOwned;

pub use store::*;

use ph::fmph::{GOBuildConf, GOConf, GOFunction};
use rkyv::ser::serializers::{
    AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer,
};

pub type MultiMap<K, V> = ImmutableMap<K, MultiStorage<V>>;
pub type RkyvMap<K, V> = ImmutableMap<K, RkyvStorage<V>>;

#[cfg(feature = "persistence")]
#[derive(serde::Serialize, serde::Deserialize)]
pub struct MapHeader<K, S> {
    keys: Vec<K>,
    hasher: Vec<u8>,
    store_header: S,
}

// i think this is the worst trait bound to ever be
pub trait SerializableToFile:
    rkyv::Serialize<
        CompositeSerializer<WriteSerializer<BufWriter<File>>, AllocScratch, SharedSerializeMap>,
    > + Clone
{
}
impl<T> SerializableToFile for T where
    T: rkyv::Serialize<
            CompositeSerializer<WriteSerializer<BufWriter<File>>, AllocScratch, SharedSerializeMap>,
        > + Clone
{
}

pub struct ImmutableMap<K: Hash, S: Storage> {
    hasher: GOFunction,
    keys: Vec<K>,
    store: S,
}

impl<K: Hash + Sync + Send + Clone + PartialEq + Debug, V: bytemuck::Pod>
    ImmutableMap<K, MultiStorage<V>>
{
    pub fn multi_from_map(
        map: HashMap<K, Vec<V>>,
        file: File,
    ) -> io::Result<ImmutableMap<K, MultiStorage<V>>> {
        let (keys, vals): (Vec<_>, Vec<_>) = map.into_iter().unzip();
        ImmutableMap::build_multi(keys, &vals, file)
    }

    pub fn build_multi(
        keys: Vec<K>,
        vals: &[impl AsRef<[V]>],
        file: File,
    ) -> io::Result<ImmutableMap<K, MultiStorage<V>>> {
        assert!(keys.len() == vals.len());

        let mut conf = GOBuildConf::with_lsize(GOConf::default(), 300);
        conf.cache_threshold = 0;

        let hasher = GOFunction::from_slice_with_conf(&keys, conf);

        let mut storage_builder = MultiStorageBuilder::new(vals.len(), file);
        // let mut reordered_vals = Vec::with_capacity(vals.len());
        let mut reordered_keys: Vec<K> = Vec::with_capacity(keys.len());

        for (k, v) in keys.into_iter().zip(vals.iter()) {
            let new_idx = hasher.get(&k).unwrap() as usize;
            storage_builder.serialize(new_idx, v.as_ref())?;
            // reordered_vals.spare_capacity_mut()[new_idx].write(v.as_ref().to_vec());
            reordered_keys.spare_capacity_mut()[new_idx].write(k);
        }

        unsafe {
            reordered_keys.set_len(vals.len());
            // reordered_vals.set_len(vals.len());
        }

        // let storage = MultiStorage::build(&reordered_vals, file)?;
        Ok(ImmutableMap {
            hasher,
            keys: reordered_keys,
            store: storage_builder.finish()?,
        })
    }
}

impl<K: Hash + Sync + Send + Clone + PartialEq + Debug, V: SerializableToFile>
    ImmutableMap<K, RkyvStorage<V>>
{
    pub fn rkyv_from_map(
        map: HashMap<K, V>,
        file: File,
    ) -> io::Result<ImmutableMap<K, RkyvStorage<V>>> {
        let (keys, vals): (Vec<_>, Vec<_>) = map.into_iter().unzip();
        ImmutableMap::build_rkyv(keys, &vals, file)
    }

    pub fn build_rkyv(
        keys: Vec<K>,
        vals: &[V],
        file: File,
    ) -> io::Result<ImmutableMap<K, RkyvStorage<V>>> {
        assert!(keys.len() == vals.len());

        let mut conf = GOBuildConf::with_lsize(GOConf::default(), 300);
        conf.cache_threshold = 0;

        let hasher = GOFunction::from_slice_with_conf(&keys, conf);

        let mut reordered_keys: Vec<K> = Vec::with_capacity(keys.len());

        let mut archiver = RkyvStorageBuilder::create(keys.len(), file)?;

        for (k, v) in keys.into_iter().zip(vals.iter().cloned()) {
            let new_idx = hasher.get(&k).unwrap() as usize;
            archiver.serialize(new_idx, &v)?;
            reordered_keys.spare_capacity_mut()[new_idx].write(k);
        }

        unsafe {
            reordered_keys.set_len(vals.len());
        }

        let storage = archiver.finish()?;
        Ok(ImmutableMap {
            hasher,
            keys: reordered_keys,
            store: storage,
        })
    }
}

#[cfg(feature = "persistence")]
impl<K: Hash + serde::Serialize, S: PersistentStorage> ImmutableMap<K, S> {
    pub fn into_header(self) -> MapHeader<K, S::Header> {
        let mut hasher = Vec::with_capacity(self.hasher.write_bytes());
        self.hasher.write(&mut hasher).unwrap();

        MapHeader {
            keys: self.keys,
            hasher,
            store_header: self.store.header(),
        }
    }
}

#[cfg(feature = "persistence")]
impl<K: Hash + DeserializeOwned, S: PersistentStorage> ImmutableMap<K, S> {
    pub fn load(header: &[u8], store: File) -> io::Result<ImmutableMap<K, S>> {
        let header: MapHeader<K, S::Header> =
            postcard::from_bytes(header).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

        let hasher = GOFunction::read(&mut header.hasher.as_slice()).unwrap();

        Ok(ImmutableMap {
            hasher,
            keys: header.keys,
            store: S::load(header.store_header, store)?,
        })
    }
}

impl<K: Hash + Sync + Send + Clone + PartialEq + Debug, S: Storage> ImmutableMap<K, S> {
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<S::Item<'_>> {
        let idx = self.hasher.get(key)?;
        if &self.keys[idx as usize] != key {
            return None;
        }

        Some(self.store.get(idx as usize))
    }
}
