use arrow::array::{ArrayRef, StructArray};
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
        from_block: 19_123_123,
        to_block: Some(19_123_223),
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
    }
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
    let data: ArrayRef = data.try_into_arrow().unwrap();
    let batch = RecordBatch::from(data.as_any().downcast_ref::<StructArray>().unwrap());

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

    let mut batches = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(batches.len(), 1);

    let batch = batches.remove(0);

    let array_ref: ArrayRef = Arc::new(StructArray::from(batch));

    array_ref.try_into_collection().unwrap()
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
