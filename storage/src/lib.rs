use std::collections::HashMap;
use std::hash::Hash;
use std::io::{self, BufWriter};
use std::{fmt::Debug, fs::File};

pub mod store;

pub use store::*;

use ph::fmph::{GOBuildConf, GOConf, GOFunction};
use rkyv::ser::serializers::{
    AllocScratch, CompositeSerializer, SharedSerializeMap, WriteSerializer,
};

pub type MultiMap<K, V> = ImmutableMap<K, MultiStorage<V>>;
pub type RkyvMap<K, V> = ImmutableMap<K, RkyvStorage<V>>;

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
