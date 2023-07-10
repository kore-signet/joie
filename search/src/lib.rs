use bytemuck::Pod;
use query::Query;
use rkyv::Archive;
use searcher::{SearchEngine, SearchResult};

use storage::RkyvMap;
use term_map::FrozenTermMap;

pub mod builder;
pub mod highlight;
mod id_list;
pub mod query;
pub mod searcher;
pub mod sentence;
pub mod term_map;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, rkyv::Serialize, rkyv::Archive)]
#[archive_attr(derive(Debug))]
pub struct CopyableRange {
    pub start: usize,
    pub end: usize,
}

pub type Token = CopyableRange;

pub struct Database<Document: Archive, DocumentMetadata: Pod, SentenceMetadata: Archive> {
    search: SearchEngine<DocumentMetadata, SentenceMetadata>,
    documents: RkyvMap<u32, Document>,
    term_map: FrozenTermMap,
}

impl<D: Archive, DM: Pod, SM: Archive> Database<D, DM, SM> {
    #[inline(always)]
    pub fn tokenize_phrase(&self, query: &str) -> Vec<u32> {
        self.term_map.tokenize_phrase(query)
    }

    #[inline(always)]
    pub fn query<'a>(
        &'a self,
        query: &'a (impl Query<DM, SM> + Send + Sync),
    ) -> impl Iterator<Item = SearchResult<'a, SM>> + 'a {
        self.search.query(query)
    }

    #[inline(always)]
    pub fn get_doc(&self, doc_id: &u32) -> Option<&<D as Archive>::Archived> {
        self.documents.get(doc_id)
    }
}
