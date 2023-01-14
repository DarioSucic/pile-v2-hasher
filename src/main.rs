#![feature(iter_collect_into)]

use std::{fs::DirEntry, path::Path, time::Instant};

use arrow::{
    array::{Array, StringArray},
    ipc::{reader::StreamReader}, record_batch::RecordBatch,
};
use rayon::prelude::*;
use xxhash_rust::xxh3::xxh3_64;

fn get_text_column<'a>(record_batch: &'a RecordBatch) -> Option<&'a StringArray> {
    let schema = record_batch.schema();
    let idx = schema.index_of("text").ok()?;
    return record_batch.column(idx).as_any().downcast_ref::<StringArray>();
}

fn hash_text(text: Option<&str>) -> u64 {
    if let Some(t) = text {
        xxh3_64(t.as_bytes())
    } else {
        unreachable!("text should never be None")
    }
}

fn calculate_hashes(arrow_file_path: &Path) -> Vec<u64> {
    let file = std::fs::File::open(arrow_file_path).unwrap();
    let mut stream_reader = StreamReader::try_new(file, None).unwrap();

    let record_batch = stream_reader.next().unwrap().unwrap();
    let texts = get_text_column(&record_batch).unwrap();

    let mut hashes = Vec::with_capacity(texts.len());
    texts.iter().map(hash_text).collect_into(&mut hashes);

    hashes
}

fn main() {
    let ds_path: String = std::env::var("DS_PATH").unwrap(); // pile-v2-eda/cache_ds
    let ds_folders: Vec<DirEntry> = std::fs::read_dir(ds_path).unwrap().filter_map(|p| p.ok()).collect();
    
    let st = Instant::now();
    ds_folders.into_par_iter().for_each(|dir_entry| {
        let mut arrow_file_path = dir_entry.path();
        arrow_file_path.push("dataset.arrow");
        let hashes = calculate_hashes(&arrow_file_path);
        println!("{:?} -> {}", dir_entry.file_name(), hashes.len());
    });
    let dt = st.elapsed();

    println!("Computed hashes in: {dt:?}");
}
