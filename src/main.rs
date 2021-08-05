
use arrow::json;
use structopt::StructOpt;
use std::path::PathBuf;
use structopt::clap::arg_enum;

use serde_json::to_string_pretty;
use std::fs::File;

use parquet::{ arrow::ArrowWriter, basic::Compression, errors::ParquetError, file::properties::WriterProperties};

/*arg_enum! {

    #[derive(Debug)]
    enum Encodings {
        PLAIN,
        RLE,
        BIT_PACKED,
        DELTA_BINARY_PACKED,
        DELTA_LENGTH_BYTE_ARRAY,
        DELTA_BYTE_ARRAY,
        RLE_DICTIONARY
    }
}*/

arg_enum! {

    #[derive(Debug)]
    enum Compressions {
        UNCOMPRESSED,
        SNAPPY,
        GZIP,
        LZO,
        BROTLI,
        LZ4,
        ZSTD
    }
}


#[derive(StructOpt, Debug)]
struct Opt {

    /// Input file
    #[structopt(short, long, parse(from_os_str))]
    input: PathBuf,

    /// Output file
    #[structopt(short, long, default_value = "output_")]
    output: String,

    #[structopt(short="d", long)]
    enable_dict: bool,

    #[structopt(short, long, default_value = "128")]
    file_size: usize,

    #[structopt(short, long, default_value = "128")]
    block_size: usize,

    #[structopt(short, long, default_value = "1")]
    page_size: usize,

    #[structopt(short="s", long, default_value = "1")]
    dict_page_size: usize,

    #[structopt(long)]
    sample_size: Option<usize>,

    #[structopt(short, long, possible_values = &Compressions::variants(), case_insensitive = true, default_value = "UNCOMPRESSED")]
    compression: Compressions,

  //  #[structopt(short, long, possible_values = &Encodings::variants(), case_insensitive = true, default_value = "PLAIN")]
   // encoding: Encodings,
}


fn convert(opts: Opt) -> Result<(), ParquetError> {
    let file = File::open(opts.input).unwrap();
    let builder = json::ReaderBuilder::new().infer_schema(opts.sample_size);
    let reader = builder.build::<File>(file).unwrap();

    let schema_pretty = to_string_pretty(&reader.schema().to_json()).unwrap();
    println!("Inferred Schema\n\n {}", schema_pretty);

    let mut properties = WriterProperties::builder()
        .set_dictionary_enabled(opts.enable_dict)
        .set_statistics_enabled(true);


    properties = properties.set_data_pagesize_limit(opts.page_size);
    properties = properties.set_dictionary_pagesize_limit(opts.dict_page_size);
    properties = properties.set_max_row_group_size(opts.block_size);



    let i = match opts.compression {
        Compressions::UNCOMPRESSED => Compression::UNCOMPRESSED,
        Compressions::SNAPPY => Compression::SNAPPY,
        Compressions::GZIP => Compression::GZIP,
        Compressions::LZO => Compression::LZO,
        Compressions::BROTLI => Compression::BROTLI,
        Compressions::LZ4 => Compression::LZ4,
        Compressions::ZSTD => Compression::ZSTD,
    };

    properties = properties.set_compression(i);


    /*
    if let j = opts.encoding {
        let i = match j {
            Encodings::PLAIN => Encoding::PLAIN,
            Encodings::RLE => Encoding::RLE,
            Encodings::BIT_PACKED => Encoding::BIT_PACKED,
            Encodings::DELTA_BINARY_PACKED => Encoding::DELTA_BINARY_PACKED,
            Encodings::DELTA_LENGTH_BYTE_ARRAY => Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Encodings::DELTA_BYTE_ARRAY => Encoding::DELTA_BYTE_ARRAY,
            Encodings::RLE_DICTIONARY => Encoding::RLE_DICTIONARY,
        };

        properties = properties.set_encoding(i);
    }*/

    let file_counter = 0;
    let out_file = PathBuf::from(opts.output+&file_counter.to_string()+".parquet");

    let output = File::create(&out_file)?;
    let mut writer = ArrowWriter::try_new(output, reader.schema(), Some(properties.build()))?;
    
        for recs in reader {
            match recs {
                Ok(recs) => writer.write(&recs)?,
                Err(err) => return Err(err.into())
            }
        }

    match writer.close() {
        Ok(_) => Ok(()),
        Err(err) => Err(err),
    }

}

fn main() {

    let opt = Opt::from_args();

    match convert(opt){
        Ok(_) => {}
        Err(err) => println!("{:?}", err)
    }

}
