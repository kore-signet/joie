use std::fmt;

use logos::{Logos, SpannedIter};
use smartstring::{LazyCompact, SmartString};

use crate::{
    query::{DocumentFilter, IntersectingQuery, PhraseQuery, QueryBuilder, UnionQuery},
    term_map::FrozenTermMap,
    DocumentMetadata, SentenceMetadata,
};

use super::{DynamicQuery, IntersectingPhraseQuery};

#[derive(Debug, Logos, Clone, Copy)]
pub enum QueryToken<'a> {
    #[regex(r#""([^"\\]|\\t|\\u|\\n|\\")*""#, |l| &l.slice()[1..l.slice().len()-1])]
    QuotedString(&'a str),
    #[regex(r#"([^"\s)(]+)"#)]
    Ident(&'a str),
    #[token("(")]
    ParenOpen,
    #[token(")")]
    ParenClose,
    #[regex(r#"AND|and|&&"#)]
    And,
    #[regex(r#"OR|or|\|\|"#)]
    Or,
    #[regex(r"\s", logos::skip)]
    InvalidToken,
}

impl<'a> fmt::Display for QueryToken<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub type Spanned<Tok, Loc, Error> = Result<(Loc, Tok, Loc), Error>;

pub enum LexicalError {
    InvalidToken,
}

pub struct Lexer<'input> {
    token_stream: SpannedIter<'input, QueryToken<'input>>,
}

impl<'input> Lexer<'input> {
    pub fn new(input: &'input str) -> Self {
        Self {
            token_stream: QueryToken::lexer(input).spanned(),
        }
    }
}

impl<'input> Iterator for Lexer<'input> {
    type Item = Spanned<QueryToken<'input>, usize, LexicalError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.token_stream.next().map(|(token, span)| match token {
            Ok(t) => Ok((span.start, t, span.end)),
            Err(_) => Err(LexicalError::InvalidToken),
        })
    }
}

#[derive(Debug, Clone)]
pub enum Expression {
    Literal(SmartString<LazyCompact>),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
}

impl Expression {
    pub fn parse<
        'a,
        D: DocumentMetadata + 'a,
        S: SentenceMetadata + 'static,
        DF: DocumentFilter<D> + Clone + 'static,
    >(
        self,
        terms: &FrozenTermMap,
        doc_filter: DF,
        optimize: bool,
    ) -> DynamicQuery<D, S, DF> {
        match self {
            Expression::Literal(v) => QueryBuilder::start(&terms.tokenize_phrase(&v))
                .filter_documents(doc_filter)
                .phrases()
                .into(),
            Expression::And(lhs, rhs) => match (*lhs, *rhs) {
                (Expression::Literal(lhs), Expression::Literal(rhs)) if optimize => {
                    let lhs_filter = doc_filter.clone();
                    let lhs: PhraseQuery<D, S, _> =
                        QueryBuilder::start(&terms.tokenize_phrase(&lhs))
                            .filter_documents(lhs_filter)
                            .phrases();
                    let rhs_filter = doc_filter.clone();
                    let rhs: PhraseQuery<D, S, _> =
                        QueryBuilder::start(&terms.tokenize_phrase(&rhs))
                            .filter_documents(rhs_filter)
                            .phrases();

                    IntersectingPhraseQuery::from_iter([lhs, rhs], doc_filter).into()
                }
                (lhs, rhs) => {
                    let lhs = lhs.parse(terms, doc_filter.clone(), optimize);
                    let rhs = rhs.parse(terms, doc_filter.clone(), optimize);
                    IntersectingQuery::from_boxed([lhs, rhs], doc_filter).into()
                }
            },
            Expression::Or(lhs, rhs) => match (*lhs, *rhs) {
                (Expression::Literal(lhs), Expression::Literal(rhs)) if optimize => {
                    let (lhs_terms, rhs_terms) =
                        (terms.tokenize_phrase(&lhs), terms.tokenize_phrase(&rhs));

                    if lhs_terms.len() == 1 && rhs_terms.len() == 1 {
                        let query_terms = vec![lhs_terms[0], rhs_terms[0]];
                        QueryBuilder::start(&query_terms)
                            .filter_documents(doc_filter)
                            .keywords()
                            .into()
                    } else {
                        let _filter = doc_filter.clone();

                        let lhs = QueryBuilder::start(&lhs_terms)
                            .filter_documents(doc_filter.clone())
                            .phrases();

                        let rhs = QueryBuilder::start(&rhs_terms)
                            .filter_documents(doc_filter)
                            .phrases();

                        UnionQuery::from_dynamic([lhs, rhs]).into()
                    }
                }
                (lhs, rhs) => {
                    let lhs = lhs.parse(terms, doc_filter.clone(), optimize);
                    let rhs = rhs.parse(terms, doc_filter, optimize);
                    UnionQuery::from_dynamic([lhs, rhs]).into()
                }
            },
        }
    }

    //     let mut terms: Vec<u32> = Vec::new();
    //     match self {
    //         Expression::Literal(_) => todo!(),
    //         Expression::And(_, _) => todo!(),
    //         Expression::Or(_, _) => todo!(),
    //     }
    // }
}

peg::parser! {
    pub grammar query_grammar<'a>() for [QueryToken<'a>] {
        pub rule expression() -> Expression
            = and()

        #[cache_left_rec]
        rule and() -> Expression
            = l:and() [QueryToken::And] r:or() { Expression::And(Box::new(l), Box::new(r))}
            / or()

        #[cache_left_rec]
        rule or() -> Expression
            = l:or() [QueryToken::Or] r:atom() { Expression::Or(Box::new(l), Box::new(r)) }
            / atom()

        rule atom() -> Expression
            = literal()
            / [QueryToken::ParenOpen] v:and() [QueryToken::ParenClose] { v }

        rule literal() -> Expression
            = [QueryToken::QuotedString(v)] { Expression::Literal(v.into()) }
            / l:(ident()+) { Expression::Literal(l.join(" ").into()) }

        rule ident() -> SmartString<LazyCompact>
            = [QueryToken::Ident(v)] { v.into() }
    }
}

// pub struct Query
