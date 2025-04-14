use arrow::array::{ArrayRef, BinaryArray, StructArray, UInt64Array, builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_convert::{
    ArrowDeserialize, ArrowField, ArrowSerialize, deserialize::TryIntoCollection,
    serialize::TryIntoArrow,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use std::time::Instant;

use cherry_core::ingest;
use ingest::evm;

use futures_lite::StreamExt;

use tikv_jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    // Get all transactions in a 100 block range
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

    for _ in 0..4 {
        // ARROW_IPC (Buffer compression: ZSTD)
        let start = Instant::now();
        let arrow_ipc = to_arrow_ipc(&transactions);
        println!(
            "took {}ms to serialize arrow_ipc",
            start.elapsed().as_millis()
        );

        println!(
            "arrow ipc serialized size: {}kB",
            arrow_ipc.len() / (1 << 10)
        );

        let start = Instant::now();
        let ipc_data = from_arrow_ipc(&arrow_ipc);
        println!(
            "took {}ms to deserialize arrow_ipc",
            start.elapsed().as_millis()
        );

        assert_eq!(&transactions, &ipc_data);

        // JSON (ETH-Hex, GZIP)
        let start = Instant::now();
        let json = to_json(&transactions);
        println!("took {}ms to serialize json", start.elapsed().as_millis());

        println!("json serialized size: {}kB", json.len() / (1 << 10));

        let start = Instant::now();
        let json_data = from_json(&json);
        println!("took {}ms to deserialize json", start.elapsed().as_millis());

        assert_eq!(&transactions, &json_data);

        // Parquet (ZSTD)
        let start = Instant::now();
        let parquet = to_parquet(&transactions);
        println!(
            "took {}ms to serialize parquet ",
            start.elapsed().as_millis()
        );

        println!("parquet serialized size: {}kB", parquet.len() / (1 << 10));

        let start = Instant::now();
        let parquet_data = from_parquet(parquet);
        println!(
            "took {}ms to deserialize parquet",
            start.elapsed().as_millis()
        );

        assert_eq!(&transactions, &parquet_data);
    }
}

fn to_parquet(data: &[Transaction]) -> Vec<u8> {
    use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterOptions};
    use parquet::file::properties::WriterProperties;

    let data: ArrayRef = data.try_into_arrow().unwrap();
    let batch = RecordBatch::from(data.as_any().downcast_ref::<StructArray>().unwrap());

    let properties = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).unwrap(),
        ))
        .build();

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

fn to_json(data: &[Transaction]) -> Vec<u8> {
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use std::io::prelude::*;

    let json_data = serde_json::to_vec(&data).unwrap();

    let mut e = GzEncoder::new(Vec::new(), Compression::default());
    e.write_all(&json_data).unwrap();

    e.finish().unwrap()
}

fn from_json(data: &[u8]) -> Vec<Transaction> {
    use flate2::read::GzDecoder;
    use std::io::prelude::*;

    let mut d = GzDecoder::new(data);
    let mut out = Vec::new();
    d.read_to_end(&mut out).unwrap();

    simd_json::from_slice(&mut out).unwrap()
}

fn to_arrow_ipc(data: &[Transaction]) -> Vec<u8> {
    let batch = Transaction::to_arrow(data);

    let mut buf = Vec::new();
    let mut writer = arrow::ipc::writer::FileWriter::try_new_with_options(
        &mut buf,
        batch.schema_ref(),
        arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::ZSTD))
            .unwrap(),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    buf
}

fn from_arrow_ipc(data: &[u8]) -> Vec<Transaction> {
    let reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(data), None).unwrap();

    let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(batches.len(), 1);

    let batch = batches.first().unwrap();

    Transaction::from_arrow(batch)
}

#[derive(
    Debug, Serialize, Deserialize, ArrowField, ArrowSerialize, ArrowDeserialize, PartialEq,
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
