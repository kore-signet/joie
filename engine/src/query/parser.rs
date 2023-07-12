use std::fmt;

use logos::{Logos, SpannedIter};
use smartstring::{LazyCompact, SmartString};
use yoke::Yoke;

use crate::{
    query::{DocumentFilter, IntersectingQuery, PhraseQuery, Query, QueryBuilder, UnionQuery},
    term_map::FrozenTermMap,
    DocumentMetadata, SentenceMetadata,
};

use super::{YokedIntersectingPhraseQuery, YokedKeywordsQuery, YokedPhraseQuery};

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
    pub fn parse<'a, D: DocumentMetadata + 'a, S: SentenceMetadata + 'static>(
        self,
        terms: &FrozenTermMap,
        doc_filter: impl DocumentFilter<D> + Clone + 'static,
        optimize: bool,
    ) -> Box<dyn Query<D, S> + Send + Sync> {
        match self {
            Expression::Literal(v) => Box::new(YokedPhraseQuery {
                inner: Yoke::attach_to_cart(terms.tokenize_phrase(&v), |q| {
                    QueryBuilder::start(q)
                        .filter_documents(doc_filter)
                        .phrases()
                }),
            }),
            Expression::And(lhs, rhs) => match (*lhs, *rhs) {
                (Expression::Literal(lhs), Expression::Literal(rhs)) if optimize => {
                    let lhs_filter = doc_filter.clone();
                    let lhs: Yoke<PhraseQuery<'_, D, S, _>, Vec<u32>> =
                        Yoke::attach_to_cart(terms.tokenize_phrase(&lhs), |t| {
                            QueryBuilder::start(t)
                                .filter_documents(lhs_filter)
                                .phrases()
                        });
                    let rhs_filter = doc_filter.clone();
                    let rhs: Yoke<PhraseQuery<'_, D, S, _>, Vec<u32>> =
                        Yoke::attach_to_cart(terms.tokenize_phrase(&rhs), |t| {
                            QueryBuilder::start(t)
                                .filter_documents(rhs_filter)
                                .phrases()
                        });

                    Box::new(YokedIntersectingPhraseQuery::from_iter(
                        [lhs, rhs],
                        doc_filter,
                    ))
                }
                (lhs, rhs) => {
                    let lhs = lhs.parse(terms, doc_filter.clone(), optimize);
                    let rhs = rhs.parse(terms, doc_filter, optimize);
                    Box::new(IntersectingQuery::from_boxed([lhs, rhs]))
                }
            },
            Expression::Or(lhs, rhs) => match (*lhs, *rhs) {
                (Expression::Literal(lhs), Expression::Literal(rhs)) if optimize => {
                    let (lhs_terms, rhs_terms) =
                        (terms.tokenize_phrase(&lhs), terms.tokenize_phrase(&rhs));

                    if lhs_terms.len() == 1 && rhs_terms.len() == 1 {
                        let query_terms = vec![lhs_terms[0], rhs_terms[0]];
                        Box::new(YokedKeywordsQuery {
                            inner: Yoke::attach_to_cart(query_terms, |t| {
                                QueryBuilder::start(t)
                                    .filter_documents(doc_filter)
                                    .keywords()
                            }),
                        })
                    } else {
                        let filter = doc_filter.clone();
                        let lhs = Box::new(YokedPhraseQuery {
                            inner: Yoke::attach_to_cart(lhs_terms, |q| {
                                QueryBuilder::start(q).filter_documents(filter).phrases()
                            }),
                        });

                        let rhs: Box<dyn Query<D, S> + Send + Sync> = Box::new(YokedPhraseQuery {
                            inner: Yoke::attach_to_cart(rhs_terms, |q| {
                                QueryBuilder::start(q)
                                    .filter_documents(doc_filter)
                                    .phrases()
                            }),
                        });

                        Box::new(UnionQuery::from_boxed([lhs, rhs]))
                    }
                }
                (lhs, rhs) => {
                    let lhs = lhs.parse(terms, doc_filter.clone(), optimize);
                    let rhs = rhs.parse(terms, doc_filter, optimize);
                    Box::new(UnionQuery::from_boxed([lhs, rhs]))
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
