#[cfg(feature = "persistence")]
use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::Path,
};

use logos::Logos;
use query::{parser::QueryToken, DocumentFilter, DynQuery, Query, QueryBuilder, YokedPhraseQuery};
use rkyv::Archive;
use searcher::{SearchEngine, SearchResult};

use storage::{RkyvMap, SerializableToFile};

#[cfg(feature = "persistence")]
use crate::sentence::{Sentence, SentenceId};
#[cfg(feature = "persistence")]
use storage::{MultiMap, PersistentStorage, SimpleStorage};

use term_map::FrozenTermMap;
use yoke::Yoke;

pub mod builder;
pub mod highlight;
mod id_list;
pub mod query;
pub mod searcher;
pub mod sentence;
pub mod term_map;

pub trait DocumentMetadata: bytemuck::Pod + Default + Send + Sync {}
impl<T> DocumentMetadata for T where T: bytemuck::Pod + Default + Send + Sync {}

pub trait SentenceMetadata:
    rkyv::Archive + SerializableToFile + Default + Send + Sync + Clone
{
}
impl<T> SentenceMetadata for T where
    T: rkyv::Archive + SerializableToFile + Default + Send + Sync + Clone
{
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, rkyv::Serialize, rkyv::Archive)]
#[archive_attr(derive(Debug))]
pub struct CopyableRange {
    pub start: usize,
    pub end: usize,
}

pub type Token = CopyableRange;

pub struct Database<Document, DM, SM>
where
    Document: Archive,
    DM: DocumentMetadata,
    SM: SentenceMetadata,
{
    search: SearchEngine<DM, SM>,
    documents: RkyvMap<u32, Document>,
    term_map: FrozenTermMap,
}

impl<D, DM, SM> Database<D, DM, SM>
where
    D: Archive,
    DM: DocumentMetadata,
    SM: SentenceMetadata + 'static,
{
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

    pub fn parse_query<F: DocumentFilter<DM> + Clone + 'static>(
        &self,
        query: &str,
        document_filter: F,
        optimize: bool,
    ) -> Option<DynQuery<DM, SM>> {
        let tokens: Vec<QueryToken<'_>> = QueryToken::lexer(query)
            .collect::<Result<Vec<QueryToken<'_>>, _>>()
            .ok()?;
        let expr = query::parser::query_grammar::expression(&tokens).ok()?;

        Some(DynQuery {
            inner: expr.parse(&self.term_map, document_filter, optimize),
        })
    }

    pub fn phrase_query<F: DocumentFilter<DM> + Clone + 'static>(
        &self,
        query: &str,
        document_filter: F,
    ) -> DynQuery<DM, SM> {
        let tokens = self.term_map.tokenize_phrase(query);
        DynQuery {
            inner: Box::new(YokedPhraseQuery {
                inner: Yoke::attach_to_cart(tokens, |tokens| {
                    QueryBuilder::start(tokens)
                        .filter_documents(document_filter)
                        .phrases()
                }),
            }),
        }
    }
}

#[cfg(feature = "persistence")]
impl<D, DM, SM> Database<D, DM, SM>
where
    D: Archive,
    DM: DocumentMetadata,
    SM: SentenceMetadata + 'static,
{
    pub fn persist(self, dir: impl AsRef<Path>) -> io::Result<()> {
        let dir = dir.as_ref();
        let headers = dir.join("headers/");
        let _ = std::fs::create_dir_all(&headers);

        fn write_ser(v: &impl serde::Serialize, path: impl AsRef<Path>) -> io::Result<()> {
            let mut out = BufWriter::new(File::create(path)?);
            let ser = postcard::to_stdvec(v)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            out.write_all(&ser)?;
            out.flush()?;
            Ok(())
        }

        let SearchEngine {
            doc_meta,
            sentences,
            index,
        } = self.search;

        write_ser(&doc_meta.header(), headers.join("doc_meta.header.joie"))?;
        write_ser(
            &sentences.into_header(),
            headers.join("sentences.header.joie"),
        )?;
        write_ser(
            &index.into_header(),
            headers.join("sentence_index.header.joie"),
        )?;
        write_ser(
            &self.documents.into_header(),
            headers.join("documents.header.joie"),
        )?;
        write_ser(&self.term_map, headers.join("term_map.joie"))?;

        Ok(())
    }

    pub fn load(dir: impl AsRef<Path>) -> io::Result<Database<D, DM, SM>> {
        use std::fs;

        let dir = dir.as_ref();
        let headers = dir.join("headers/");

        let sentence_index: MultiMap<u32, SentenceId> = MultiMap::load(
            &fs::read(headers.join("sentence_index.header.joie"))?,
            File::open(dir.join("sentences.index.joie"))?,
        )?;

        let sentence_store: RkyvMap<SentenceId, Sentence<SM>> = RkyvMap::load(
            &fs::read(headers.join("sentences.header.joie"))?,
            File::open(dir.join("sentences.storage.joie"))?,
        )?;

        let doc_store: RkyvMap<u32, D> = RkyvMap::load(
            &fs::read(headers.join("documents.header.joie"))?,
            File::open(dir.join("documents.storage.joie"))?,
        )?;

        let metadata_header: <SimpleStorage<DM> as PersistentStorage>::Header =
            postcard::from_bytes(&fs::read(headers.join("doc_meta.header.joie"))?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let metadata_store: SimpleStorage<DM> = SimpleStorage::load(
            metadata_header,
            File::open(dir.join("documents.fast.joie"))?,
        )?;

        let term_map: FrozenTermMap =
            postcard::from_bytes(&fs::read(headers.join("term_map.joie"))?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Database {
            search: SearchEngine {
                doc_meta: metadata_store,
                sentences: sentence_store,
                index: sentence_index,
            },
            documents: doc_store,
            term_map,
        })
    }
}
