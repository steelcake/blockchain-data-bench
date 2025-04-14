use arrow::array::{FixedSizeBinaryArray, builder};
use arrow::datatypes::DataType;
use arrow_convert::deserialize::ArrowArrayIterable;
use arrow_convert::{
    ArrowDeserialize, ArrowField, ArrowSerialize, deserialize::TryIntoCollection,
    serialize::TryIntoArrow,
};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

fn main() {
}

type Hash = [u8; 32];
type Address = [u8; 20];

#[derive(Serialize, Deserialize, ArrowField, ArrowSerialize, ArrowDeserialize)]
struct Transaction {
    #[serde(serialize_with = "serialize_hash")]
    #[serde(deserialize_with = "deserialize_hash")]
    block_hash: Hash,
    block_number: u64,
    #[serde(serialize_with = "serialize_address")]
    #[serde(deserialize_with = "deserialize_address")]
    from: Address,
    #[serde(serialize_with = "serialize_hash")]
    #[serde(deserialize_with = "deserialize_hash")]
    hash: Hash,
    #[serde(serialize_with = "serialize_binary")]
    #[serde(deserialize_with = "deserialize_binary")]
    input: Vec<u8>,
    #[serde(serialize_with = "serialize_address")]
    #[serde(deserialize_with = "deserialize_address")]
    to: Address,
    transaction_index: u64,
}

fn serialize_address<S>(x: &Address, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&encode_hex(x.as_slice()))
}

fn serialize_hash<S>(x: &Hash, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&encode_hex(x.as_slice()))
}

fn serialize_binary<S>(x: &Vec<u8>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&encode_hex(x.as_slice()))
}

fn deserialize_address<'de, D>(d: D) -> Result<Address, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(d)?;
    let buf = decode_hex(value.as_str());
    Ok(buf.as_slice().try_into().unwrap())
}

fn deserialize_hash<'de, D>(d: D) -> Result<Hash, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(d)?;
    let buf = decode_hex(&value);
    Ok(buf.as_slice().try_into().unwrap())
}

fn deserialize_binary<'de, D>(d: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(d)?;
    Ok(decode_hex(&value))
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
