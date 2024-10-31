use parquet::{
    file::{
        properties::WriterProperties,
        writer::SerializedFileWriter,
    },
    schema::parser::parse_message_type,
};
use std::fs::File;
use std::sync::Arc;

pub fn write_parquet_file() -> Result<(), Box<dyn std::error::Error>> {
    // Define the schema
    let message_type = "
        message schema {
            REQUIRED INT32 index;
            REQUIRED BINARY type (UTF8);
            REQUIRED BINARY sample (UTF8);
            OPTIONAL BINARY extras (UTF8);
        }
    ";

    let schema = Arc::new(parse_message_type(message_type)?);

    // Create a file to write to
    let file = File::create("example.parquet")?;

    // Create a writer
    let props = WriterProperties::builder().build();
    let mut writer = SerializedFileWriter::new(file, schema, Arc::new(props))?;

    // Write some sample data
    let mut row_group_writer = writer.next_row_group()?;

    // Write index column
    let mut index_writer = row_group_writer.next_column()?.unwrap();
    index_writer.typed::<parquet::data_type::Int32Type>().write_batch(&[0, 1, 2], None, None)?;

    // Write type column
    let mut type_writer = row_group_writer.next_column()?.unwrap();
    type_writer.typed::<parquet::data_type::ByteArrayType>().write_batch(
        &["Type A", "Type B", "Type C"].iter().map(|&s| s.into()).collect::<Vec<_>>(),
        None,
        None,
    )?;

    // Write sample column
    let mut sample_writer = row_group_writer.next_column()?.unwrap();
    sample_writer.typed::<parquet::data_type::ByteArrayType>().write_batch(
        &["Sample 1", "Sample 2", "Sample 3"].iter().map(|&s| s.into()).collect::<Vec<_>>(),
        None,
        None,
    )?;

    // Write extras column (with some null values)
    let mut extras_writer = row_group_writer.next_column()?.unwrap();
    extras_writer.typed::<parquet::data_type::ByteArrayType>().write_batch(
        &[r#"{"key": "value"}"#, "", r#"{"another": "json"}"#].iter().map(|&s| s.into()).collect::<Vec<_>>(),
        Some(&[1, 0, 1]),
        None,
    )?;

    // Close the writers
    row_group_writer.close()?;
    writer.close()?;

    println!("Parquet file created successfully!");
    Ok(())
}
