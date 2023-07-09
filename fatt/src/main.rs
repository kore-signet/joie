use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fs::{File, OpenOptions},
    io,
    path::Path,
};

use fatt::{DownloadOptions, EpMetadata, Season, SeasonId, SentenceMetadata, StoredEpisode};
use joie::{
    sentence::{Sentence, SentenceId, SentencePart},
    term_map::TermMap,
    Db,
};
use storage::{MultiMap, RkyvMap, SimpleStorage};

fn print_highlights(parts: &[SentencePart<'_>]) {
    for part in parts {
        match part {
            SentencePart::Normal(s) => print!("{s}"),
            SentencePart::Highlight(s) => print!("*{s}*"),
        }
    }
}

fn open_mapfile(path: impl AsRef<Path>) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
}

#[derive(Default)]
struct IndexBuilder {
    sentence_map: HashMap<SentenceId, Sentence<SentenceMetadata>>,
    term_to_sentence: HashMap<u32, Vec<SentenceId>>,
    term_map: TermMap,
}

impl IndexBuilder {
    fn add_document(&mut self, ep_id: u32, text: &str) {
        for (sentence_idx, sentence) in self
            .term_map
            .tokenize_all(text, |_| ())
            .into_iter()
            .enumerate()
        {
            let id = SentenceId::new(ep_id, sentence_idx as u32);
            assert!(bytemuck::cast::<SentenceId, u64>(id) != 0);

            for (_, token) in sentence.tokens.iter().enumerate() {
                let entry = self
                    .term_to_sentence
                    .entry(token.term)
                    .or_insert_with(Vec::new);

                entry.push(SentenceId {
                    doc: ep_id,
                    sentence: sentence_idx as u32,
                    // token_idx: token_idx as u32,
                });
            }

            self.sentence_map.insert(id, sentence);
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut builder = IndexBuilder::default();

    let mut ep_storage: HashMap<u32, StoredEpisode> = HashMap::new();
    let mut ep_metadata: BTreeMap<u32, EpMetadata> = BTreeMap::new();

    let seasons: BTreeMap<SeasonId, Season> =
        serde_json::from_str(include_str!("../../data/seasons.json"))?;

    for Season { id, episodes, .. } in seasons.into_values() {
        let season_id = id;

        for episode in episodes {
            let Some(DownloadOptions { plain }) = episode.download else { continue };
            let text = std::fs::read_to_string(Path::new("data/").join(plain))?;
            let ep_id: u32 = ((season_id as u32 + 1) * 1000) + episode.sorting_number as u32;

            println!("ID: {ep_id} - {}", &episode.title);

            builder.add_document(ep_id, &text);
            ep_metadata.insert(
                ep_id,
                EpMetadata {
                    season: season_id as u8,
                },
            );
            ep_storage.insert(
                ep_id,
                StoredEpisode {
                    title: episode.title,
                    slug: episode.slug,
                    docs_id: episode.docs_id,
                },
            );
        }
    }

    let IndexBuilder {
        sentence_map,
        mut term_to_sentence,
        mut term_map,
    } = builder;

    for (_, val) in term_to_sentence.iter_mut() {
        val.sort();
        val.dedup();
    }

    let sentence_index: MultiMap<u32, SentenceId> =
        MultiMap::multi_from_map(term_to_sentence, open_mapfile("index.joie")?)?;
    let sentence_store: RkyvMap<SentenceId, Sentence<()>> =
        RkyvMap::rkyv_from_map(sentence_map, open_mapfile("sentences.storage.joie")?)?;
    let ep_storage: RkyvMap<u32, StoredEpisode> =
        RkyvMap::rkyv_from_map(ep_storage, open_mapfile("documents.storage.joie")?)?;

    let mut metadata_array =
        vec![EpMetadata { season: 0 }; *ep_metadata.last_key_value().unwrap().0 as usize + 1];
    for (idx, meta) in ep_metadata {
        metadata_array[idx as usize] = meta;
    }

    let metadata_store =
        SimpleStorage::build(&metadata_array, open_mapfile("documents.fast.joie")?)?;

    let db = Db {
        doc_meta: metadata_store,
        sentences: sentence_store,
        index: sentence_index,
    };

    let query = term_map.tokenize_query("any sound");
    // dbg!(db.query_phrase(&query).len());

    for sentence in db.query_phrase(&query, None::<fn(&EpMetadata) -> bool>, |_| true) {
        println!(
            "--/ {} /--",
            ep_storage.get(&sentence.id.doc).unwrap().title
        );
        print_highlights(&sentence.parts);
        println!();
        println!();
    }

    for sentence in db.query_phrase(
        &query,
        Some(|meta: &EpMetadata| meta.season == SeasonId::TwilightMirage as u8),
        |_| true,
    ) {
        println!(
            "--/ {} /--",
            ep_storage.get(&sentence.id.doc).unwrap().title
        );
        print_highlights(&sentence.parts);
        println!();
        println!();
    }

    println!("bench time");

    println!(
        "lookup: {}",
        easybench::bench(|| { db.query_phrase(&query, None::<fn(&EpMetadata) -> bool>, |_| true) })
    );

    println!(
        "lookup (hieron only): {}",
        easybench::bench(|| {
            db.query_phrase(
                &query,
                Some(|meta: &EpMetadata| meta.season == SeasonId::AutumnInHieron as u8),
                |_| true,
            )
        })
    );

    Ok(())
}
