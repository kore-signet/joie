use crate::{sentence::ArchivedSentence, DocumentMetadata, SentenceMetadata};

pub trait SentenceFilter<S: SentenceMetadata>: Send + Sync {
    fn filter_sentence(&self, sentence: &ArchivedSentence<S>) -> bool;
}

pub trait DocumentFilter<D: DocumentMetadata>: Send + Sync {
    fn filter_document(&self, document_meta: &D) -> bool;

    fn needed() -> bool {
        true
    }
}

impl<T, D: DocumentMetadata> DocumentFilter<D> for T
where
    T: Fn(&D) -> bool + Send + Sync,
{
    #[inline(always)]
    fn filter_document(&self, document_meta: &D) -> bool {
        self(document_meta)
    }
}

impl<D: DocumentMetadata> DocumentFilter<D> for () {
    #[inline(always)]
    fn filter_document(&self, _document_meta: &D) -> bool {
        true
    }

    fn needed() -> bool {
        false
    }
}

impl<T, S: SentenceMetadata> SentenceFilter<S> for T
where
    T: Fn(&ArchivedSentence<S>) -> bool + Send + Sync,
{
    #[inline(always)]
    fn filter_sentence(&self, sentence: &ArchivedSentence<S>) -> bool {
        self(sentence)
    }
}
