// use std::collections::{BTreeMap, HashMap};
// use std::error::Error;

// use std::path::{Path, PathBuf};

// pub mod db;
// pub mod store;
// use store::*;

// use crate::db::{Db, Sentence, SentenceId, TermMap, print_highlights};

use std::path::PathBuf;

#[derive(serde::Deserialize, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
#[serde(rename_all = "kebab-case")]
pub enum SeasonId {
    AutumnInHieron = 0,
    Marielda = 1,
    WinterInHieron = 2,
    SpringInHieron = 3,
    Counterweight = 4,
    TwilightMirage = 5,
    RoadToPartizan = 6,
    Partizan = 7,
    RoadToPalisade = 8,
    Palisade = 9,
    Sangfielle = 10,
    Extras = 11,
    Patreon = 12,
    Other = 13,
}

#[derive(serde::Deserialize)]
pub struct Season {
    pub title: String,
    pub id: SeasonId,
    pub episodes: Vec<Episode>,
}

#[derive(serde::Deserialize)]
pub struct Episode {
    pub title: String,
    pub slug: String,
    pub done: bool,
    pub sorting_number: usize,
    pub docs_id: Option<String>,
    pub download: Option<DownloadOptions>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, Default)]
pub struct StoredEpisode {
    pub title: String,
    pub slug: String,
    pub docs_id: Option<String>,
}

#[derive(serde::Deserialize)]
pub struct DownloadOptions {
    pub plain: PathBuf,
}

#[derive(
    Clone, Default, PartialEq, Eq, PartialOrd, Ord, Copy, bytemuck::Pod, bytemuck::Zeroable,
)]
#[repr(transparent)]
pub struct EpMetadata {
    pub season: u8,
}

pub type SentenceMetadata = ();
