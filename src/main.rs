use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryDictionaryBuilder, DictionaryArray, PrimitiveRunBuilder,
    RunArray, StructArray, UInt64Array, builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, UInt64Type};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_convert::{
    ArrowDeserialize, ArrowField, ArrowSerialize, deserialize::TryIntoCollection,
    serialize::TryIntoArrow,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use std::time::Instant;

use cherry_core::ingest;
use flatbuffers::FlatBufferBuilder;
use ingest::evm;

use futures_lite::StreamExt;

use tikv_jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod olive;

#[allow(warnings)]
#[path = "./transaction_generated.rs"]
mod transaction_generated;
use crate::transaction_generated::transaction::{
    TransactionData, TransactionList, TransactionListArgs, root_as_transaction_list,
};
use transaction_generated::transaction::TransactionDataArgs;

#[derive(Copy, Clone)]
enum JsonCompression {
    None,
    Gzip,
    Zstd(i32),
}

#[tokio::main]
async fn main() {
    let transactions = fetch_transactions().await;

    for _ in 0..4 {
        // Benchmark JSON with different compression options
        benchmark_json(&transactions, JsonCompression::None); // No compression
        benchmark_json(&transactions, JsonCompression::Gzip); // GZIP compression
        benchmark_json(&transactions, JsonCompression::Zstd(1)); // ZSTD level 1
        benchmark_json(&transactions, JsonCompression::Zstd(3)); // ZSTD level 3
        benchmark_json(&transactions, JsonCompression::Zstd(9)); // ZSTD level 9
        println!("---");

        // Benchmark Arrow IPC without compression
        benchmark_arrow_ipc(&transactions, false, false);
        // Benchmark Arrow IPC with ZSTD compression
        benchmark_arrow_ipc(&transactions, true, false);
        // Benchmark Arrow IPC with light compression
        benchmark_arrow_ipc(&transactions, false, true);
        println!("---");

        // Parquet with different compression levels
        benchmark_parquet(&transactions, None); // No compression
        benchmark_parquet(&transactions, Some(1)); // Fastest compression
        benchmark_parquet(&transactions, Some(3)); // Balanced compression
        benchmark_parquet(&transactions, Some(9)); // Best compression
        println!("---");

        // Benchmark FlatBuffers implementation
        benchmark_flatbuffers(&transactions);
        // Benchmark FlatBuffers implementation with ZSTD compression
        benchmark_flatbuffers_compressed(&transactions, 1); // ZSTD level 1
        benchmark_flatbuffers_compressed(&transactions, 3); // ZSTD level 3
        benchmark_flatbuffers_compressed(&transactions, 9); // ZSTD level 9

        // Benchmark olive
        benchmark_olive(&transactions);

        println!("\n====================================================\n");
    }
}

async fn fetch_transactions() -> Vec<Transaction> {
    let query = ingest::Query::Evm(evm::Query {
        from_block: 20_123_123,
        to_block: Some(20_123_223),
        transactions: vec![evm::TransactionRequest::default()],
        fields: evm::Fields {
            transaction: evm::TransactionFields::all(),
            ..Default::default()
        },
        ..Default::default()
    });

    let mut stream = ingest::start_stream(
        ingest::ProviderConfig::new(ingest::ProviderKind::Hypersync),
        query,
    )
    .await
    .unwrap();

    let mut transactions = Vec::<Transaction>::new();

    while let Some(res) = stream.next().await {
        let mut res = res.unwrap();
        let tx_data = res.remove("transactions").unwrap();
        let tx_data = tx_data.project(&[0, 1, 2, 5, 6, 8, 9]).unwrap();
        let struct_arr = StructArray::from(tx_data);
        let tx_data = arrow_convert::deserialize::arrow_array_deserialize_iterator::<Transaction>(
            &struct_arr,
        )
        .unwrap();
        transactions.extend(tx_data);
    }

    transactions
}

fn benchmark_olive(transactions: &[Transaction]) {
    let start = Instant::now();
    let olive_data = to_olive(&transactions);

    println!("[OLIVE]");
    println!("\tserialization:    {}ms", start.elapsed().as_millis());
    println!("\tserialized size:  {}kB", olive_data.len() / (1 << 10));

    let start = Instant::now();
    let mut olive_data = from_olive(&olive_data);

    println!("\tdeserialization:  {}ms", start.elapsed().as_millis());

    for (left, right) in transactions.iter().zip(olive_data.iter()) {
        assert_eq!(left, right);
    }
}

fn benchmark_arrow_ipc(
    transactions: &[Transaction],
    use_compression: bool,
    light_compressed: bool,
) {
    let compression_label = if use_compression {
        ", ZSTD"
    } else if light_compressed {
        ", light_compressed"
    } else {
        ""
    };

    // Assume tx inputs are stored individually zstd compressed in storage in light_compressed case
    let mut transactions_input = transactions.to_vec();
    // let max_input_size = transactions
    //     .iter()
    //     .filter_map(|tx| tx.input.as_ref().map(|i| i.len()))
    //     .max()
    //     .unwrap_or(0);
    if light_compressed {
        for tx in transactions_input.iter_mut() {
            if let Some(input) = tx.input.as_ref() {
                tx.input = Some(
                    lz4::block::compress(
                        &input,
                        Some(lz4::block::CompressionMode::HIGHCOMPRESSION(9)),
                        true,
                    )
                    .unwrap(),
                );
                // tx.input = Some(zstd::bulk::compress(&input, 8).unwrap());
            }
        }
    }

    let start = Instant::now();
    let arrow_ipc = to_arrow_ipc(&transactions_input, use_compression, light_compressed);

    println!("[ARROW-IPC{}]", compression_label,);
    println!("\tserialization:    {}ms", start.elapsed().as_millis());
    println!("\tserialized size:  {}kB", arrow_ipc.len() / (1 << 10));

    let start = Instant::now();
    let mut ipc_data = from_arrow_ipc(&arrow_ipc, light_compressed);

    if light_compressed {
        for tx in ipc_data.iter_mut() {
            if let Some(input) = tx.input.as_ref() {
                // tx.input = Some(zstd::bulk::decompress(&input, max_input_size).unwrap());
                tx.input = Some(lz4::block::decompress(&input, None).unwrap());
            }
        }
    }

    println!("\tdeserialization:  {}ms", start.elapsed().as_millis());

    for (left, right) in transactions.iter().zip(ipc_data.iter()) {
        assert_eq!(left, right);
    }
}

fn benchmark_parquet(transactions: &[Transaction], compression_level: Option<i32>) {
    let compression_label = match compression_level {
        Some(level) => format!(", ZSTD_{}", level),
        None => "".to_string(),
    };

    let start = Instant::now();
    let parquet = to_parquet(&transactions, compression_level);

    println!("[PARQUET{}]", compression_label,);
    println!("\tserialization:    {}ms", start.elapsed().as_millis());
    println!("\tserialized size:  {}kB", parquet.len() / (1 << 10));

    let start = Instant::now();
    let parquet_data = from_parquet(parquet);

    println!("\tdeserialization:  {}ms", start.elapsed().as_millis());

    assert_eq!(&transactions, &parquet_data);
}

fn benchmark_json(transactions: &[Transaction], compression: JsonCompression) {
    let compression_label = match compression {
        JsonCompression::None => "".to_string(),
        JsonCompression::Gzip => ", GZIP".to_string(),
        JsonCompression::Zstd(level) => format!(", ZSTD_{}", level),
    };

    let start = Instant::now();
    let json_data = to_json(&transactions, compression);

    println!("[JSON{}]", compression_label,);
    println!("\tserialization:    {}ms", start.elapsed().as_millis());
    println!("\tserialized size:  {}kB", json_data.len() / (1 << 10));

    let start = Instant::now();
    let deserialized = from_json(&json_data, compression);
    println!("\tdeserialization:  {}ms", start.elapsed().as_millis());

    assert_eq!(&transactions, &deserialized);
}

fn benchmark_flatbuffers(transactions: &[Transaction]) {
    let start = Instant::now();
    let flatbuffers_data = Transaction::to_flatbuffer(&transactions);

    println!("[FLATBUFFERS]");
    println!("\tserialization:    {}ms", start.elapsed().as_millis());
    println!(
        "\tserialized size:  {}kB",
        flatbuffers_data.len() / (1 << 10)
    );

    let start = Instant::now();
    let deserialized = Transaction::from_flatbuffer(&flatbuffers_data);
    println!("\tdeserialization:  {}ms", start.elapsed().as_millis());

    assert_eq!(&transactions, &deserialized);
}

fn benchmark_flatbuffers_compressed(transactions: &[Transaction], level: i32) {
    let start = Instant::now();
    let data = to_flatbuffers_compressed(&transactions, level);

    println!("[FLATBUFFERS-CUSTOM, ZSTD_{}]", level);
    println!("\tserialization:    {}ms", start.elapsed().as_millis());
    println!("\tserialized size:  {}kB", data.len() / (1 << 10));

    let start = Instant::now();
    let deserialized = from_flatbuffers_compressed(&data);
    println!("\tdeserialization:  {}ms", start.elapsed().as_millis());

    assert_eq!(&transactions, &deserialized);
}

fn to_parquet(data: &[Transaction], compression_level: Option<i32>) -> Vec<u8> {
    use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
    use parquet::file::properties::WriterProperties;

    let data: ArrayRef = data.try_into_arrow().unwrap();
    let batch = RecordBatch::from(data.as_any().downcast_ref::<StructArray>().unwrap());

    let mut properties_builder = WriterProperties::builder();
    if let Some(level) = compression_level {
        properties_builder = properties_builder.set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(level).unwrap(),
        ));
    }
    let properties = properties_builder.build();

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(
        &mut buf,
        batch.schema(),
        ArrowWriterOptions::new().with_properties(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    std::mem::drop(writer);

    buf
}

fn from_parquet(data: Vec<u8>) -> Vec<Transaction> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let data = bytes::Bytes::from_owner(data);

    let builder = ParquetRecordBatchReaderBuilder::try_new(data).unwrap();
    let reader = builder.build().unwrap();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    let batch = arrow::compute::concat_batches(batches[0].schema_ref(), batches.iter()).unwrap();

    let array_ref: ArrayRef = Arc::new(StructArray::from(batch));

    array_ref.try_into_collection().unwrap()
}

fn to_json(data: &[Transaction], compression: JsonCompression) -> Vec<u8> {
    let json_data = serde_json::to_vec(&data).unwrap();

    match compression {
        JsonCompression::None => json_data,
        JsonCompression::Gzip => {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::prelude::*;

            let mut e = GzEncoder::new(Vec::new(), Compression::default());
            e.write_all(&json_data).unwrap();
            e.finish().unwrap()
        }
        JsonCompression::Zstd(level) => {
            use std::io::prelude::*;
            use zstd::stream::write::Encoder;

            let mut e = Encoder::new(Vec::new(), level).unwrap();
            e.write_all(&json_data).unwrap();
            e.finish().unwrap()
        }
    }
}

fn from_json(data: &[u8], compression: JsonCompression) -> Vec<Transaction> {
    let mut decompressed = match compression {
        JsonCompression::None => data.to_vec(),
        JsonCompression::Gzip => {
            use flate2::read::GzDecoder;
            use std::io::prelude::*;

            let mut d = GzDecoder::new(data);
            let mut out = Vec::new();
            d.read_to_end(&mut out).unwrap();
            out
        }
        JsonCompression::Zstd(_) => {
            use std::io::prelude::*;
            use zstd::stream::read::Decoder;

            let mut d = Decoder::new(data).unwrap();
            let mut out = Vec::new();
            d.read_to_end(&mut out).unwrap();
            out
        }
    };

    simd_json::from_slice(&mut decompressed).unwrap()
}

#[repr(C)]
struct SerializeOutput {
    len: u32,
    alloc_len: u32,
    ptr: *mut u8,
}

#[link(name = "olivers")]
unsafe extern "C" {
    fn olivers_ffi_serialize(
        array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
    ) -> SerializeOutput;

    fn olivers_ffi_deserialize(
        data: SerializeOutput,
        array_out: *mut FFI_ArrowArray,
        schema_out: *mut FFI_ArrowSchema,
    );
}

fn to_olive(data: &[Transaction]) -> &'static [u8] {
    let batch = Transaction::to_arrow(data);

    let arr_data = StructArray::from(batch).into_data();
    let (mut ffi_arr, mut ffi_schema) = arrow::ffi::to_ffi(&arr_data).unwrap();

    let out = unsafe { olivers_ffi_serialize(&mut ffi_arr, &mut ffi_schema) };

    println!("ANAN");

    let out = unsafe { std::slice::from_raw_parts(out.ptr, usize::try_from(out.len).unwrap()) };

    std::mem::forget(ffi_arr);
    std::mem::forget(ffi_schema);

    out
}

fn from_olive(data: &'static [u8]) -> Vec<Transaction> {
    // unsafe { olivers_ffi_deserialize(SerializeOutput, array_out, schema_out);}

    todo!()
}

fn to_arrow_ipc(data: &[Transaction], use_compression: bool, light_compressed: bool) -> Vec<u8> {
    let batch = if light_compressed {
        Transaction::to_arrow_light_compressed(data)
    } else {
        Transaction::to_arrow(data)
    };

    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::FileWriter::try_new_with_options(
        &mut buf,
        batch.schema_ref(),
        arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(if use_compression {
                Some(arrow::ipc::CompressionType::ZSTD)
                // Some(arrow::ipc::CompressionType::LZ4_FRAME)
            } else {
                None
            })
            .unwrap(),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    buf
}

fn from_arrow_ipc(data: &[u8], light_compressed: bool) -> Vec<Transaction> {
    let reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(data), None).unwrap();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(batches.len(), 1);

    let batch = batches.first().unwrap();

    if light_compressed {
        Transaction::from_arrow_light_compressed(batch)
    } else {
        Transaction::from_arrow(batch)
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, ArrowField, ArrowSerialize, ArrowDeserialize, PartialEq,
)]
struct Transaction {
    #[serde(serialize_with = "serialize_binary")]
    #[serde(deserialize_with = "deserialize_binary")]
    block_hash: Option<Vec<u8>>,
    block_number: Option<u64>,
    #[serde(serialize_with = "serialize_binary")]
    #[serde(deserialize_with = "deserialize_binary")]
    from: Option<Vec<u8>>,
    #[serde(serialize_with = "serialize_binary")]
    #[serde(deserialize_with = "deserialize_binary")]
    hash: Option<Vec<u8>>,
    #[serde(serialize_with = "serialize_binary")]
    #[serde(deserialize_with = "deserialize_binary")]
    input: Option<Vec<u8>>,
    #[serde(serialize_with = "serialize_binary")]
    #[serde(deserialize_with = "deserialize_binary")]
    to: Option<Vec<u8>>,
    transaction_index: Option<u64>,
}

impl Transaction {
    fn from_arrow_light_compressed(batch: &RecordBatch) -> Vec<Self> {
        let block_hash = batch
            .column_by_name("block_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap()
            .downcast_dict::<BinaryArray>()
            .unwrap();
        let block_number = batch
            .column_by_name("block_number")
            .unwrap()
            .as_any()
            .downcast_ref::<RunArray<Int32Type>>()
            .unwrap()
            .downcast::<UInt64Array>()
            .unwrap();
        let from = batch
            .column_by_name("from")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap()
            .downcast_dict::<BinaryArray>()
            .unwrap();
        let hash = batch
            .column_by_name("hash")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let input = batch
            .column_by_name("input")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let to = batch
            .column_by_name("to")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap()
            .downcast_dict::<BinaryArray>()
            .unwrap();
        let transaction_index = batch
            .column_by_name("transaction_index")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut out = Vec::with_capacity(batch.num_rows());

        for (block_hash, block_number, from, hash, input, to, transaction_index) in itertools::izip!(
            block_hash.into_iter(),
            block_number.into_iter(),
            from.into_iter(),
            hash.iter(),
            input.iter(),
            to.into_iter(),
            transaction_index.iter()
        ) {
            out.push(Transaction {
                block_hash: block_hash.map(|x| x.to_vec()),
                block_number,
                from: from.map(|x| x.to_vec()),
                hash: hash.map(|x| x.to_vec()),
                input: input.map(|x| x.to_vec()),
                to: to.map(|x| x.to_vec()),
                transaction_index,
            });
        }

        out
    }

    fn to_arrow_light_compressed(data: &[Self]) -> RecordBatch {
        let mut block_hash = BinaryDictionaryBuilder::<Int32Type>::new();
        let mut block_number = PrimitiveRunBuilder::<Int32Type, UInt64Type>::new();
        let mut from = BinaryDictionaryBuilder::<Int32Type>::new();
        let mut hash = builder::BinaryBuilder::new();
        let mut input = builder::BinaryBuilder::with_capacity(
            data.len(),
            data.iter()
                .map(|tx| tx.input.as_ref().map(|x| x.len()).unwrap_or_default())
                .sum(),
        );
        let mut to = BinaryDictionaryBuilder::<Int32Type>::new();
        let mut transaction_index = builder::UInt64Builder::new();

        for tx in data.iter() {
            block_hash.append_option(tx.block_hash.as_ref());
            block_number.append_option(tx.block_number);
            from.append_option(tx.from.as_ref());
            hash.append_option(tx.hash.as_ref());
            input.append_option(tx.input.as_ref());
            to.append_option(tx.to.as_ref());
            transaction_index.append_option(tx.transaction_index);
        }

        let fields: Vec<Arc<dyn Array>> = vec![
            Arc::new(block_hash.finish()),
            Arc::new(block_number.finish()),
            Arc::new(from.finish()),
            Arc::new(hash.finish()),
            Arc::new(input.finish()),
            Arc::new(to.finish()),
            Arc::new(transaction_index.finish()),
        ];

        let field_names = [
            "block_hash",
            "block_number",
            "from",
            "hash",
            "input",
            "to",
            "transaction_index",
        ];

        RecordBatch::try_new(
            Arc::new(Schema::new(
                field_names
                    .iter()
                    .zip(fields.iter())
                    .map(|(field_name, field)| {
                        Field::new(*field_name, field.data_type().clone(), true)
                    })
                    .collect::<Vec<_>>(),
            )),
            fields,
        )
        .unwrap()
    }

    fn from_arrow(batch: &RecordBatch) -> Vec<Self> {
        let block_hash = batch
            .column_by_name("block_hash")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let block_number = batch
            .column_by_name("block_number")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let from = batch
            .column_by_name("from")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let hash = batch
            .column_by_name("hash")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let input = batch
            .column_by_name("input")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let to = batch
            .column_by_name("to")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let transaction_index = batch
            .column_by_name("transaction_index")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let mut out = Vec::with_capacity(batch.num_rows());

        for (block_hash, block_number, from, hash, input, to, transaction_index) in itertools::izip!(
            block_hash.iter(),
            block_number.iter(),
            from.iter(),
            hash.iter(),
            input.iter(),
            to.iter(),
            transaction_index.iter()
        ) {
            out.push(Transaction {
                block_hash: block_hash.map(|x| x.to_vec()),
                block_number,
                from: from.map(|x| x.to_vec()),
                hash: hash.map(|x| x.to_vec()),
                input: input.map(|x| x.to_vec()),
                to: to.map(|x| x.to_vec()),
                transaction_index,
            });
        }

        out
    }

    fn to_arrow(data: &[Self]) -> RecordBatch {
        let mut block_hash = builder::BinaryBuilder::new();
        let mut block_number = builder::UInt64Builder::new();
        let mut from = builder::BinaryBuilder::new();
        let mut hash = builder::BinaryBuilder::new();
        let mut input = builder::BinaryBuilder::with_capacity(
            data.len(),
            data.iter()
                .map(|tx| tx.input.as_ref().map(|x| x.len()).unwrap_or_default())
                .sum(),
        );
        let mut to = builder::BinaryBuilder::new();
        let mut transaction_index = builder::UInt64Builder::new();

        for tx in data.iter() {
            block_hash.append_option(tx.block_hash.as_ref());
            block_number.append_option(tx.block_number);
            from.append_option(tx.from.as_ref());
            hash.append_option(tx.hash.as_ref());
            input.append_option(tx.input.as_ref());
            to.append_option(tx.to.as_ref());
            transaction_index.append_option(tx.transaction_index);
        }

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("block_hash", DataType::Binary, true),
                Field::new("block_number", DataType::UInt64, true),
                Field::new("from", DataType::Binary, true),
                Field::new("hash", DataType::Binary, true),
                Field::new("input", DataType::Binary, true),
                Field::new("to", DataType::Binary, true),
                Field::new("transaction_index", DataType::UInt64, true),
            ])),
            vec![
                Arc::new(block_hash.finish()),
                Arc::new(block_number.finish()),
                Arc::new(from.finish()),
                Arc::new(hash.finish()),
                Arc::new(input.finish()),
                Arc::new(to.finish()),
                Arc::new(transaction_index.finish()),
            ],
        )
        .unwrap()
    }

    fn to_flatbuffer(data: &[Self]) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();

        let tx_data = data
            .iter()
            .map(|tx| {
                let block_hash = tx.block_hash.as_ref().map(|x| builder.create_vector(x));
                let from = tx.from.as_ref().map(|x| builder.create_vector(x));
                let hash = tx.hash.as_ref().map(|x| builder.create_vector(x));
                let input = tx.input.as_ref().map(|x| builder.create_vector(x));
                let to = tx.to.as_ref().map(|x| builder.create_vector(x));

                TransactionData::create(
                    &mut builder,
                    &TransactionDataArgs {
                        block_hash,
                        block_number: tx.block_number.unwrap_or_default(),
                        from,
                        hash,
                        input,
                        to,
                        transaction_index: tx.transaction_index.unwrap_or_default(),
                    },
                )
            })
            .collect::<Vec<_>>();

        let tx_vector = builder.create_vector(&tx_data);

        let root = TransactionList::create(
            &mut builder,
            &TransactionListArgs {
                transactions: Some(tx_vector),
            },
        );

        builder.finish(root, None);

        builder.finished_data().to_vec()
    }

    fn from_flatbuffer(data: &[u8]) -> Vec<Self> {
        let list = root_as_transaction_list(data).unwrap();

        let mut out: Vec<Self> = Vec::with_capacity(list.transactions().iter().len());

        for tx in list.transactions().unwrap().iter() {
            out.push(Transaction {
                block_hash: tx.block_hash().map(|x| x.bytes().to_vec()),
                block_number: Some(tx.block_number()),
                from: tx.from().map(|x| x.bytes().to_vec()),
                hash: tx.hash().map(|x| x.bytes().to_vec()),
                input: tx.input().map(|x| x.bytes().to_vec()),
                to: tx.to().map(|x| x.bytes().to_vec()),
                transaction_index: Some(tx.transaction_index()),
            })
        }

        out
    }
}

fn serialize_binary<S>(x: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match x {
        Some(v) => s.serialize_str(&encode_hex(v.as_slice())),
        None => s.serialize_none(),
    }
}

fn deserialize_binary<'de, D>(d: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(d)?;
    match value {
        Some(v) => Ok(Some(decode_hex(&v))),
        None => Ok(None),
    }
}

fn encode_hex(buf: &[u8]) -> String {
    format!("0x{}", faster_hex::hex_string(buf))
}

fn decode_hex(value: &str) -> Vec<u8> {
    let hex = value.strip_prefix("0x").unwrap();

    let len = hex.len();
    let mut dst = vec![0; len / 2];

    faster_hex::hex_decode(hex.as_bytes(), &mut dst).unwrap();

    dst
}

// Compress FlatBuffers data with ZSTD
fn to_flatbuffers_compressed(data: &[Transaction], level: i32) -> Vec<u8> {
    use std::io::prelude::*;
    use zstd::stream::write::Encoder;

    let flatbuffers_data = Transaction::to_flatbuffer(data);

    let mut encoder = Encoder::new(Vec::new(), level).unwrap();
    encoder.write_all(&flatbuffers_data).unwrap();
    encoder.finish().unwrap()
}

// Decompress and deserialize FlatBuffers data
fn from_flatbuffers_compressed(data: &[u8]) -> Vec<Transaction> {
    use std::io::prelude::*;
    use zstd::stream::read::Decoder;

    let mut decoder = Decoder::new(data).unwrap();
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();

    Transaction::from_flatbuffer(&decompressed)
}
