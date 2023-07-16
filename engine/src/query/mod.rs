use std::{marker::PhantomData, ops::Deref};

use yoke::{Yoke, Yokeable};

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

pub trait Query<D: DocumentMetadata, S: SentenceMetadata> {
    // is_part_of_intersect can be used to, for example, ignore dedup() in keyword queries
    fn find_sentence_ids(&self, db: &SearchEngine<D, S>, caller: CallerType) -> SentenceIdList;

    fn filter_map(&self, _result: &mut SearchResult<'_, S>) -> bool {
        true
    }

    fn find_highlights(&self, sentence: &mut SearchResult<'_, S>);
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

    pub fn phrases(self) -> PhraseQuery<'a, D, S, DF> {
        PhraseQuery {
            phrase: self.terms,
            highlighter: PhraseHighlighter::new(self.terms),
            document_filter: self.document_filter,
            spooky: PhantomData,
        }
    }

    pub fn keywords(self) -> KeywordsQuery<'a, D, S, DF> {
        KeywordsQuery {
            keywords: self.terms,
            highlighter: KeywordHighlighter::new(self.terms),
            document_filter: self.document_filter,
            spooky: PhantomData,
        }
    }
}

pub struct YokedDynQuery<D: DocumentMetadata + 'static, S: SentenceMetadata + 'static> {
    pub inner: Yoke<DynQuery<'static, D, S>, Vec<u32>>,
}

impl<D: DocumentMetadata + 'static, S: SentenceMetadata + 'static> Query<D, S>
    for YokedDynQuery<D, S>
{
    fn find_sentence_ids(
        &self,
        db: &crate::searcher::SearchEngine<D, S>,
        caller: CallerType,
    ) -> crate::id_list::SentenceIdList {
        self.inner.get().find_sentence_ids(db, caller)
    }

    fn find_highlights(&self, sentence: &mut crate::searcher::SearchResult<'_, S>) {
        self.inner.get().find_highlights(sentence)
    }

    fn filter_map(&self, result: &mut crate::searcher::SearchResult<'_, S>) -> bool {
        self.inner.get().filter_map(result)
    }
}

#[derive(Yokeable)]
#[repr(transparent)]
pub struct DynQuery<'a, D: DocumentMetadata, S: SentenceMetadata> {
    pub(crate) inner: Box<dyn Query<D, S> + Send + Sync + 'a>,
}

impl<'a, D: DocumentMetadata + 'a, S: SentenceMetadata + 'a> Query<D, S> for DynQuery<'a, D, S> {
    fn find_sentence_ids(
        &self,
        db: &crate::searcher::SearchEngine<D, S>,
        caller: CallerType,
    ) -> crate::id_list::SentenceIdList {
        self.inner.find_sentence_ids(db, caller)
    }

    fn find_highlights(&self, sentence: &mut crate::searcher::SearchResult<'_, S>) {
        self.inner.find_highlights(sentence)
    }

    fn filter_map(&self, result: &mut crate::searcher::SearchResult<'_, S>) -> bool {
        self.inner.filter_map(result)
    }
}

impl<'a, D: DocumentMetadata, S: SentenceMetadata> Deref for DynQuery<'a, D, S> {
    type Target = dyn Query<D, S> + Send + Sync + 'a;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
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
