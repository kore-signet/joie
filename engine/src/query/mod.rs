use std::marker::PhantomData;

use enum_dispatch::enum_dispatch;

mod filter;
mod intersect;
mod keywords;
mod phrase;
mod union_query;

pub use filter::*;

pub use intersect::*;
pub use keywords::*;
pub use phrase::*;
pub use union_query::*;
pub mod parser;

use crate::{
    id_list::SentenceIdList,
    searcher::{SearchEngine, SearchResult},
    DocumentMetadata, SentenceMetadata,
};

#[enum_dispatch]
pub trait Query<D: DocumentMetadata, S: SentenceMetadata> {
    // is_part_of_intersect can be used to, for example, ignore dedup() in keyword queries
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>, caller: CallerType) -> SentenceIdList;

    fn filter_map(&self, _result: &mut SearchResult<'_, S>) -> bool {
        true
    }

    fn find_highlights(&self, sentence: &mut SearchResult<'_, S>);
}

#[enum_dispatch(Query<D,S>)]
pub enum DynamicQuery<D: DocumentMetadata, S: SentenceMetadata, DF: DocumentFilter<D>> {
    Phrase(PhraseQuery<D, S, DF>),
    Keywords(KeywordsQuery<D, S, DF>),
    Intersection(IntersectingQuery<D, S, DF>),
    PhraseIntersection(IntersectingPhraseQuery<D, S, DF>),
    Union(UnionQuery<D, S, DF>),
}

#[derive(Clone, Copy)]
pub struct QueryBuilder<'a, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    terms: &'a [u32],
    document_filter: DF,
    spooky: PhantomData<(D, S)>,
}

impl<'a, D: DocumentMetadata, S: SentenceMetadata> QueryBuilder<'a, D, S, ()> {
    pub fn start(phrase: &'a [u32]) -> QueryBuilder<'a, D, S, ()> {
        QueryBuilder {
            terms: phrase,
            document_filter: (),
            spooky: PhantomData,
        }
    }
}

impl<'a, D, S, DF> QueryBuilder<'a, D, S, DF>
where
    D: DocumentMetadata,
    S: SentenceMetadata,
    DF: DocumentFilter<D>,
{
    pub fn filter_documents<NDF: DocumentFilter<D>>(
        self,
        doc_filter: NDF,
    ) -> QueryBuilder<'a, D, S, NDF> {
        QueryBuilder {
            terms: self.terms,
            document_filter: doc_filter,
            spooky: PhantomData,
        }
    }

    pub fn phrases(self) -> PhraseQuery<D, S, DF> {
        PhraseQuery {
            phrase: self.terms.into(),
            highlighter: PhraseHighlighter::new(self.terms),
            document_filter: self.document_filter,
            spooky: PhantomData,
        }
    }

    pub fn keywords(self) -> KeywordsQuery<D, S, DF> {
        KeywordsQuery {
            keywords: self.terms.into(),
            highlighter: KeywordHighlighter::new(self.terms),
            document_filter: self.document_filter,
            spooky: PhantomData,
        }
    }
}

// what is this query being called by?
#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
pub enum CallerType {
    Intersection,
    Union,
    TopLevel,
}

impl CallerType {
    pub fn intersect(&self) -> bool {
        *self == CallerType::Intersection
    }

    pub fn union(&self) -> bool {
        *self == CallerType::Union
    }

    pub fn top_level(&self) -> bool {
        *self == CallerType::TopLevel
    }
}
