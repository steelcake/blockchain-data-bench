use arrow::array::{Array, AsArray, BinaryArray, StructArray, UInt64Array, builder, make_array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vortex::Array as VortexArray;
use vortex::arrow::{FromArrowArray, IntoArrowArray};
use vortex::compressor::CompactCompressor;
use vortex::file::{VortexOpenOptions, VortexWriteOptions, WriteStrategyBuilder};
use vortex::io::runtime::single::SingleThreadRuntime;

use cherry_core::ingest;
use flatbuffers::FlatBufferBuilder;
use ingest::evm;

use futures_lite::StreamExt;

use tikv_jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

    // Benchmark JSON with different compression options
    benchmark_json(&transactions, JsonCompression::None).print("Json"); // No compression
    benchmark_json(&transactions, JsonCompression::Gzip).print("Json Gzip"); // GZIP compression
    benchmark_json(&transactions, JsonCompression::Zstd(1)).print("Json ZSTD 1"); // ZSTD level 1
    benchmark_json(&transactions, JsonCompression::Zstd(3)).print("Json ZSTD 3"); // ZSTD level 3
    benchmark_json(&transactions, JsonCompression::Zstd(9)).print("Json ZSTD 9"); // ZSTD level 9
    println!("---");

    // Benchmark Arrow IPC with ZSTD
    benchmark_arrow_ipc(&transactions).print("ArrowIPC");
    println!("---");

    // Parquet with different compression levels
    benchmark_parquet(&transactions, None).print("Parquet"); // No compression
    benchmark_parquet(&transactions, Some(1)).print("Parquet ZSTD 1"); // Fastest compression
    benchmark_parquet(&transactions, Some(3)).print("Parquet ZSTD 3"); // Balanced compression
    benchmark_parquet(&transactions, Some(9)).print("Parquet ZSTD 9"); // Best compression
    println!("---");

    // Benchmark FlatBuffers implementation
    benchmark_flatbuffers(&transactions).print("Flatbuffers");
    // Benchmark FlatBuffers implementation with ZSTD compression
    benchmark_flatbuffers_compressed(&transactions, 1).print("Flatbuffers ZSTD 1"); // ZSTD level 1
    benchmark_flatbuffers_compressed(&transactions, 3).print("Flatbuffers ZSTD 3"); // ZSTD level 3
    benchmark_flatbuffers_compressed(&transactions, 9).print("Flatbuffers ZSTD 9"); // ZSTD level 9
    println!("---");

    // Benchmark olive
    benchmark_olive(&transactions).print("OLIVE");
    println!("---");

    // Benchmark vortex
    benchmark_vortex(&transactions).await.print("VORTEX");

    println!("\n====================================================\n");
}

async fn fetch_transactions() -> RecordBatch {
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

    let mut transactions = Vec::<RecordBatch>::new();

    while let Some(res) = stream.next().await {
        let mut res = res.unwrap();
        let tx_data = res.remove("transactions").unwrap();
        let tx_data = tx_data.project(&[0, 1, 2, 5, 6, 8, 9]).unwrap();
        transactions.push(tx_data);
    }

    arrow::compute::concat_batches(&transactions[0].schema(), &transactions).unwrap()
}

struct BenchResult {
    serialization: Duration,
    deserialization: Duration,
    size: usize,
}

impl BenchResult {
    pub fn print(&self, name: &str) {
        println!("[{}]", name);
        println!("\tserialization:    {}us", self.serialization.as_micros());
        println!(
            "\tdeserialization:    {}us",
            self.deserialization.as_micros()
        );
        println!("\tserialized size:  {}kB", self.size / (1 << 10));
    }
}

fn benchmark_olive(transactions: &RecordBatch) -> BenchResult {
    let start = Instant::now();
    let olive_data = to_olive(transactions);

    let serialization = start.elapsed();
    let size = olive_data.len();

    let start = Instant::now();
    let olive_data = from_olive(olive_data);

    let deserialization = start.elapsed();

    let expected = Transaction::from_arrow(transactions);
    let got = Transaction::from_arrow(&olive_data);

    for (exp, g) in expected.iter().zip(got.iter()) {
        assert_eq!(exp, g);
    }

    BenchResult {
        serialization,
        deserialization,
        size,
    }
}

async fn benchmark_vortex(transactions: &RecordBatch) -> BenchResult {
    let start = Instant::now();
    let vrtx = to_vortex(transactions);
    let serialization = start.elapsed();
    let size = vrtx.len();

    let start = Instant::now();
    let vrtx_data = from_vortex(vrtx);
    let deserialization = start.elapsed();

    assert_eq!(transactions, &vrtx_data);

    BenchResult {
        serialization,
        deserialization,
        size,
    }
}

fn benchmark_arrow_ipc(transactions: &RecordBatch) -> BenchResult {
    let start = Instant::now();
    let arrow_ipc = to_arrow_ipc(transactions);
    let serialization = start.elapsed();
    let size = arrow_ipc.len();

    let start = Instant::now();
    let ipc_data = from_arrow_ipc(&arrow_ipc);
    let deserialization = start.elapsed();

    assert_eq!(transactions, &ipc_data);

    BenchResult {
        serialization,
        deserialization,
        size,
    }
}

fn benchmark_parquet(transactions: &RecordBatch, compression_level: Option<i32>) -> BenchResult {
    let start = Instant::now();
    let parquet = to_parquet(transactions, compression_level);
    let serialization = start.elapsed();
    let size = parquet.len();

    let start = Instant::now();
    let parquet_data = from_parquet(parquet);
    let deserialization = start.elapsed();

    assert_eq!(transactions, &parquet_data);

    BenchResult {
        serialization,
        deserialization,
        size,
    }
}

fn benchmark_json(transactions: &RecordBatch, compression: JsonCompression) -> BenchResult {
    let start = Instant::now();
    let transactions_v = Transaction::from_arrow(transactions);
    let json_data = to_json(&transactions_v, compression);
    let serialization = start.elapsed();
    let size = json_data.len();

    let start = Instant::now();
    let deserialized = from_json(&json_data, compression);
    let deserialized = Transaction::to_arrow(&deserialized);
    let deserialization = start.elapsed();

    assert_eq!(transactions, &deserialized);

    BenchResult {
        serialization,
        deserialization,
        size,
    }
}

fn benchmark_flatbuffers(transactions: &RecordBatch) -> BenchResult {
    let start = Instant::now();
    let transactions_v = Transaction::from_arrow(transactions);
    let flatbuffers_data = Transaction::to_flatbuffer(&transactions_v);
    let serialization = start.elapsed();
    let size = flatbuffers_data.len();

    let start = Instant::now();
    let deserialized = Transaction::from_flatbuffer(&flatbuffers_data);
    let deserialized = Transaction::to_arrow(&deserialized);
    let deserialization = start.elapsed();

    assert_eq!(transactions, &deserialized);

    BenchResult {
        serialization,
        deserialization,
        size,
    }
}

fn benchmark_flatbuffers_compressed(transactions: &RecordBatch, level: i32) -> BenchResult {
    let start = Instant::now();
    let transactions_v = Transaction::from_arrow(transactions);
    let data = to_flatbuffers_compressed(&transactions_v, level);
    let serialization = start.elapsed();
    let size = data.len();

    let start = Instant::now();
    let deserialized = from_flatbuffers_compressed(&data);
    let deserialized = Transaction::to_arrow(&deserialized);
    let deserialization = start.elapsed();

    assert_eq!(transactions, &deserialized);

    BenchResult {
        serialization,
        deserialization,
        size,
    }
}

fn to_parquet(batch: &RecordBatch, compression_level: Option<i32>) -> Vec<u8> {
    use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
    use parquet::file::properties::WriterProperties;

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
    writer.write(batch).unwrap();
    writer.finish().unwrap();

    std::mem::drop(writer);

    buf
}

fn from_parquet(data: Vec<u8>) -> RecordBatch {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let data = bytes::Bytes::from_owner(data);

    let builder = ParquetRecordBatchReaderBuilder::try_new(data).unwrap();
    let reader = builder.build().unwrap();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    arrow::compute::concat_batches(batches[0].schema_ref(), batches.iter()).unwrap()
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

fn to_olive(batch: &RecordBatch) -> &'static [u8] {
    let arr_data = StructArray::from(batch.clone()).into_data();
    let (mut ffi_arr, mut ffi_schema) = arrow::ffi::to_ffi(&arr_data).unwrap();

    let out = unsafe { olivers_ffi_serialize(&mut ffi_arr, &mut ffi_schema) };

    let out = unsafe { std::slice::from_raw_parts(out.ptr, usize::try_from(out.len).unwrap()) };

    std::mem::forget(ffi_arr);
    std::mem::forget(ffi_schema);

    out
}

fn from_olive(data: &'static [u8]) -> RecordBatch {
    let mut ffi_arr: FFI_ArrowArray = unsafe { std::mem::zeroed() };
    let mut ffi_schema: FFI_ArrowSchema = unsafe { std::mem::zeroed() };
    unsafe {
        olivers_ffi_deserialize(
            SerializeOutput {
                ptr: data.as_ptr() as *mut _,
                len: u32::try_from(data.len()).unwrap(),
            },
            &mut ffi_arr,
            &mut ffi_schema,
        );
    }

    let arr_data = unsafe { arrow::ffi::from_ffi(ffi_arr, &ffi_schema).unwrap() };
    let arr = make_array(arr_data);

    RecordBatch::from(arr.as_struct())
}

fn to_arrow_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::FileWriter::try_new_with_options(
        &mut buf,
        batch.schema_ref(),
        arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
            .unwrap(),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();

    buf
}

fn from_arrow_ipc(data: &[u8]) -> RecordBatch {
    let reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(data), None).unwrap();

    let mut batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(batches.len(), 1);

    batches.pop().unwrap()
}

fn to_vortex(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let arr = Arc::<dyn VortexArray>::from_arrow(batch, false);
    let summary = VortexWriteOptions::default()
        .with_strategy(
            WriteStrategyBuilder::new()
                .with_compressor(CompactCompressor::default().with_zstd_level(1))
                .build(),
        )
        .with_blocking(SingleThreadRuntime::default())
        .write(&mut buf, arr.to_array_iterator())
        .unwrap();

    buf.truncate(usize::try_from(summary.size()).unwrap());

    buf
}

fn from_vortex(data: Vec<u8>) -> RecordBatch {
    let res: Vec<Arc<dyn VortexArray>> = VortexOpenOptions::default()
        .open_buffer(data)
        .unwrap()
        .scan()
        .unwrap()
        .into_array_iter(&SingleThreadRuntime::default())
        .unwrap()
        .map(|x| x.unwrap())
        .collect();

    let schema = make_schema();
    let dt = DataType::Struct(schema.fields().clone());

    let res = res
        .into_iter()
        .map(|x| {
            RecordBatch::from(
                x.into_arrow(&dt)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap(),
            )
        })
        .collect::<Vec<RecordBatch>>();

    arrow::compute::concat_batches(res[0].schema_ref(), &res).unwrap()
}

fn make_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_hash", DataType::Binary, true),
        Field::new("block_number", DataType::UInt64, true),
        Field::new("from", DataType::Binary, true),
        Field::new("hash", DataType::Binary, true),
        Field::new("input", DataType::Binary, true),
        Field::new("to", DataType::Binary, true),
        Field::new("transaction_index", DataType::UInt64, true),
    ]))
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
            make_schema(),
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
