use std::fs::File;

use parquet::file::{metadata::ParquetMetaData, reader::{FileReader, SerializedFileReader}};

pub fn create_reader_from_path(path: &str) -> SerializedFileReader<File> {
    let file = File::open(path).unwrap();
    return SerializedFileReader::new(file).unwrap();
}

pub fn get_reader_metadata(reader: &SerializedFileReader<File>) -> ParquetMetaData {
    return reader.metadata().clone();
}
