use std::{collections::BTreeMap, error::Error, path::Path};

use fatt::{DownloadOptions, EpMetadata, Season, SeasonId, SentenceMetadata, StoredEpisode};
use joie::{
    builder::{DatabaseBuilder, DocumentData},
    query::QueryBuilder,
    sentence::SentencePart,
};

fn print_highlights(parts: &[SentencePart<'_>]) {
    for part in parts {
        match part {
            SentencePart::Normal(s) => print!("{s}"),
            SentencePart::Highlight(s) => print!("*{s}*"),
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut builder: DatabaseBuilder<StoredEpisode, EpMetadata, SentenceMetadata> =
        DatabaseBuilder::default();

    let seasons: BTreeMap<SeasonId, Season> =
        serde_json::from_str(include_str!("../../data/seasons.json"))?;

    for Season { id, episodes, .. } in seasons.into_values() {
        let season_id = id;

        for episode in episodes {
            let Some(DownloadOptions { plain }) = episode.download else { continue };
            let text = std::fs::read_to_string(Path::new("data/").join(plain))?;
            let ep_id: u32 = ((season_id as u32 + 1) * 1000) + episode.sorting_number as u32;

            println!("ID: {ep_id} - {}", &episode.title);

            builder.add_document(DocumentData {
                id: ep_id,
                text: &text,
                metadata: EpMetadata {
                    season: season_id as u8,
                },
                data: StoredEpisode {
                    title: episode.title,
                    slug: episode.slug,
                    docs_id: episode.docs_id,
                },
            });
        }
    }

    let db = builder.build_in("./database")?;

    let query_phrase = db.tokenize_phrase("we could");
    // dbg!(db.query_phrase(&query).len());

    let query = QueryBuilder::start(&query_phrase);

    for sentence in db.query(&query.phrases()) {
        println!("--/ {} /--", db.get_doc(&sentence.id.doc).unwrap().title);
        print_highlights(&sentence.parts);
        println!();
        println!();
    }

    println!("--TWILIGHT MIRAGE ONLY--");

    for sentence in db.query(
        &query
            .filter_documents(|meta: &EpMetadata| meta.season == SeasonId::TwilightMirage as u8)
            .phrases(),
    ) {
        println!("--/ {} /--", db.get_doc(&sentence.id.doc).unwrap().title);
        print_highlights(&sentence.parts);
        println!();
        println!();
    }

    println!("--KEYWORDS --");

    // for sentence in db.query(&query.keywords()).take(20) {
    //     println!(
    //         "--/ {} /--",
    //         ep_storage.get(&sentence.id.doc).unwrap().title
    //     );
    //     print_highlights(&sentence.parts);
    //     println!();
    //     println!();
    // }

    println!("bench time");

    println!(
        "lookup: {}",
        easybench::bench(|| { db.query(&query.phrases()).for_each(|_| {}) })
    );

    println!(
        "lookup (twilight mirage only): {}",
        easybench::bench(|| {
            db.query(
                &query
                    .filter_documents(|meta: &EpMetadata| {
                        meta.season == SeasonId::TwilightMirage as u8
                    })
                    .phrases(),
            )
            .for_each(|_| {})
        })
    );

    println!(
        "lookup (keywords): {}",
        easybench::bench(|| { db.query(&query.keywords(),).for_each(|_| {}) })
    );
    // println!(
    //     "lookup (hieron only): {}",
    //     easybench::bench(|| {
    //         db.query_phrase(
    //             &query,
    //             Some(|meta: &EpMetadata| meta.season == SeasonId::AutumnInHieron as u8),
    //             |_| true,
    //         )
    //     })
    // );

    Ok(())
}
