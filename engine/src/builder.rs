use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io,
    path::Path,
};

use storage::{MultiMap, RkyvMap, SerializableToFile, SimpleStorage};

use crate::{
    searcher::SearchEngine,
    sentence::{Sentence, SentenceId},
    term_map::TermMap,
    Database, DocumentMetadata, SentenceMetadata,
};

fn open_mapfile(path: impl AsRef<Path>) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

#[derive(Default)]
pub struct DatabaseBuilder<D, DM, SM>
where
    D: rkyv::Archive + SerializableToFile,
    DM: DocumentMetadata,
    SM: SentenceMetadata,
{
    sentence_map: HashMap<SentenceId, Sentence<SM>>,
    term_to_sentence: HashMap<u32, Vec<SentenceId>>,
    doc_metadata: BTreeMap<u32, DM>,
    doc_storage: HashMap<u32, D>,
    make_sentence_metadata: Option<Box<dyn Fn(&str) -> SM>>,
    term_map: TermMap,
}

#[derive(Debug, Default)]
pub struct DocumentData<'a, D: rkyv::Archive + SerializableToFile, DM: DocumentMetadata> {
    pub id: u32,
    pub text: &'a str,
    pub metadata: DM,
    pub data: D,
}

impl<D, DM, SM> DatabaseBuilder<D, DM, SM>
where
    D: rkyv::Archive + SerializableToFile,
    DM: DocumentMetadata,
    SM: SentenceMetadata,
{
    pub fn set_sentence_metadata_creator(&mut self, f: impl Fn(&str) -> SM + 'static) {
        self.make_sentence_metadata = Some(Box::new(f));
    }

    pub fn add_document(&mut self, doc: DocumentData<D, DM>) {
        for (sentence_idx, sentence) in self
            .term_map
            .tokenize_all(doc.text, |v| {
                if let Some(make_metadata) = self.make_sentence_metadata.as_ref() {
                    make_metadata(v)
                } else {
                    SM::default()
                }
            })
            .into_iter()
            .enumerate()
        {
            let id = SentenceId::new(doc.id, sentence_idx as u32);
            assert!(bytemuck::cast::<SentenceId, u64>(id) != 0);

            for term in &sentence.terms {
                let entry = self.term_to_sentence.entry(*term).or_insert_with(Vec::new);

                entry.push(SentenceId {
                    doc: doc.id,
                    sentence: sentence_idx as u32,
                });
            }

            self.sentence_map.insert(id, sentence);
        }

        self.doc_storage.insert(doc.id, doc.data);
        self.doc_metadata.insert(doc.id, doc.metadata);
    }

    // pub fn build(self) -> io::Result<Database<D, DM, SM>> {
    //     self.build_in_tempdir(TempDir::new()?)
    // }

    // pub fn build_in(self, dir: impl AsRef<Path>) -> io::Result<Database<D, DM, SM>> {
    //     self.build_in_tempdir(TempDir::new_in(dir)?)
    // }

    pub fn build_in(mut self, dir: impl AsRef<Path>) -> io::Result<Database<D, DM, SM>> {
        for (_, val) in self.term_to_sentence.iter_mut() {
            val.sort();
            val.dedup();
        }

        let dir = dir.as_ref();

        let sentence_index: MultiMap<u32, SentenceId> = MultiMap::multi_from_map(
            self.term_to_sentence,
            open_mapfile(dir.join("sentences.index.joie"))?,
        )?;
        let sentence_store: RkyvMap<SentenceId, Sentence<SM>> = RkyvMap::rkyv_from_map(
            self.sentence_map,
            open_mapfile(dir.join("sentences.storage.joie"))?,
        )?;
        let doc_store: RkyvMap<u32, D> = RkyvMap::rkyv_from_map(
            self.doc_storage,
            open_mapfile(dir.join("documents.storage.joie"))?,
        )?;

        let mut metadata_array =
            vec![
                DM::default();
                self.doc_metadata.last_key_value().map_or(0, |v| *v.0) as usize + 1
            ];

        for (idx, meta) in self.doc_metadata {
            metadata_array[idx as usize] = meta;
        }

        let metadata_store = SimpleStorage::build(
            &metadata_array,
            open_mapfile(dir.join("documents.fast.joie"))?,
        )?;

        Ok(Database {
            search: SearchEngine {
                doc_meta: metadata_store,
                sentences: sentence_store,
                index: sentence_index,
            },
            documents: doc_store,
            term_map: self.term_map.freeze(),
        })
    }
}
