use logos::Logos;
use query::{DocumentFilter, DynQuery, Query, QueryBuilder, YokedPhraseQuery};
use rkyv::{ser::serializers::AllocSerializer, Archive};
use searcher::{SearchEngine, SearchResult};

use storage::RkyvMap;
use term_map::FrozenTermMap;
use yoke::Yoke;

use crate::query_parser::QueryToken;

pub mod builder;
pub mod highlight;
mod id_list;
pub mod query;
pub mod query_parser;
pub mod searcher;
pub mod sentence;
pub mod term_map;

pub trait DocumentMetadata: bytemuck::Pod + Default + Send + Sync {}
impl<T> DocumentMetadata for T where T: bytemuck::Pod + Default + Send + Sync {}

pub trait SentenceMetadata:
    rkyv::Archive + rkyv::Serialize<AllocSerializer<1024>> + Default + Send + Sync + Clone
{
}
impl<T> SentenceMetadata for T where
    T: rkyv::Archive + rkyv::Serialize<AllocSerializer<1024>> + Default + Send + Sync + Clone
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
    ) -> Option<DynQuery<DM, SM>> {
        let tokens: Vec<QueryToken<'_>> = QueryToken::lexer(query)
            .collect::<Result<Vec<QueryToken<'_>>, _>>()
            .ok()?;
        let expr = query_parser::query_grammar::expression(&tokens).ok()?;

        Some(DynQuery {
            inner: expr.parse(&self.term_map, document_filter),
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
