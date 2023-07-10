use bytemuck::Pod;
use query::Query;
use rkyv::Archive;
use searcher::SearchEngine;
use sentence::SentenceWithHighlights;
use storage::RkyvMap;
use term_map::FrozenTermMap;

pub mod builder;
pub mod highlight;
pub mod query;
pub mod searcher;
pub mod sentence;
pub mod term_map;

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
        query: &'a impl Query<DM, SM>,
    ) -> impl Iterator<Item = SentenceWithHighlights<'a, SM>> + 'a {
        self.search.query(query)
    }

    #[inline(always)]
    pub fn get_doc(&self, doc_id: &u32) -> Option<&<D as Archive>::Archived> {
        self.documents.get(doc_id)
    }
}
