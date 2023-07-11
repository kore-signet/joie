use std::fmt;

use logos::{Logos, SpannedIter};
use smartstring::{LazyCompact, SmartString};
use yoke::Yoke;

use crate::{
    query::{
        DocumentFilter, DynQuery, IntersectingQuery, Query, QueryBuilder, UnionQuery, YokedDynQuery,
    },
    term_map::FrozenTermMap,
    DocumentMetadata, SentenceMetadata,
};

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
    ) -> Box<dyn Query<D, S> + Send + Sync> {
        // the box recursion here pains me deeply
        match self {
            Expression::Literal(v) => {
                let terms = terms.tokenize_phrase(&v);
                Box::new(YokedDynQuery {
                    inner: Yoke::attach_to_cart(terms, |q| DynQuery {
                        inner: Box::new(
                            QueryBuilder::start(q)
                                .filter_documents(doc_filter)
                                .phrases(),
                        ),
                    }),
                })
            }
            Expression::And(lhs, rhs) => {
                let lhs = lhs.parse(terms, doc_filter.clone());
                let rhs = rhs.parse(terms, doc_filter);
                let mut intersect = IntersectingQuery::default();
                intersect.and_boxed(lhs);
                intersect.and_boxed(rhs);

                Box::new(intersect)
            }
            Expression::Or(lhs, rhs) => {
                let lhs = lhs.parse(terms, doc_filter.clone());
                let rhs = rhs.parse(terms, doc_filter);
                let mut join = UnionQuery::default();
                join.or_boxed(lhs);
                join.or_boxed(rhs);

                Box::new(join)
            }
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
