use std::collections::HashMap;
use std::hash::Hash;
use std::io;
use std::{fmt::Debug, fs::File};

pub mod store;
pub use store::*;

use ph::fmph::{GOBuildConf, GOConf, GOFunction};
use rkyv::ser::serializers::AllocSerializer;

pub type MultiMap<K, V> = ImmutableMap<K, MultiStorage<V>>;
pub type RkyvMap<K, V> = ImmutableMap<K, RkyvStorage<V>>;

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

        let hasher = GOFunction::from_slice_with_conf(
            &keys,
            GOBuildConf::with_lsize(GOConf::default(), 300),
        );
        let mut reordered_vals = Vec::with_capacity(vals.len());
        let mut reordered_keys: Vec<K> = Vec::with_capacity(keys.len());

        for (k, v) in keys.into_iter().zip(vals.iter()) {
            let new_idx = hasher.get(&k).unwrap() as usize;
            reordered_vals.spare_capacity_mut()[new_idx].write(v.as_ref().to_vec());
            reordered_keys.spare_capacity_mut()[new_idx].write(k);
        }

        unsafe {
            reordered_keys.set_len(vals.len());
            reordered_vals.set_len(vals.len());
        }

        let storage = MultiStorage::build(&reordered_vals, file)?;
        Ok(ImmutableMap {
            hasher,
            keys: reordered_keys,
            store: storage,
        })
    }
}

impl<
        K: Hash + Sync + Send + Clone + PartialEq + Debug,
        V: rkyv::Serialize<AllocSerializer<1024>> + Clone,
    > ImmutableMap<K, RkyvStorage<V>>
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

        let hasher = GOFunction::from_slice_with_conf(
            &keys,
            GOBuildConf::with_lsize(GOConf::default(), 300),
        );
        let mut reordered_vals = Vec::with_capacity(vals.len());
        let mut reordered_keys: Vec<K> = Vec::with_capacity(keys.len());

        for (k, v) in keys.into_iter().zip(vals.iter().cloned()) {
            let new_idx = hasher.get(&k).unwrap() as usize;
            reordered_vals.spare_capacity_mut()[new_idx].write(v);
            reordered_keys.spare_capacity_mut()[new_idx].write(k);
        }

        unsafe {
            reordered_keys.set_len(vals.len());
            reordered_vals.set_len(vals.len());
        }

        let storage = RkyvStorage::build(reordered_vals, file)?;
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
