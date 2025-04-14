use arrow_convert::{deserialize::TryIntoCollection, serialize::TryIntoArrow, ArrowField, ArrowSerialize, ArrowDeserialize};
use serde::{Serialize, Deserialize, Deserializer, Serializer};
use serde::de::{self, Visitor};
use std::fmt;
use arrow::datatypes::DataType;
use arrow::array::{builder, FixedSizeBinaryArray};
use arrow_convert::deserialize::ArrowArrayIterable;

fn main() {
    println!("Hello, world!");
}

#[derive(Serialize, Deserialize, ArrowField, ArrowSerialize, ArrowDeserialize)]
struct Transaction {
    block_hash: Hash,
    block_number: u64,
    from: Address,
    hash: Hash,
}

type Hash = FixedSizeData<32>;
type Address = FixedSizeData<20>;

pub struct FixedSizeData<const N: usize>(Box<[u8; N]>);

impl<const N: usize> FixedSizeData<N> {
    fn as_slice(&self) -> &[u8] {
        &*self.0
    }
}

impl<const N: usize> From<&[u8]> for FixedSizeData<N> {
    fn from(buf: &[u8]) -> FixedSizeData<N> {
        let buf: [u8; N] = buf.try_into().unwrap();
        FixedSizeData(Box::new(buf))
    }
}

struct FixedSizeDataVisitor<const N: usize>;

impl<'de, const N: usize> Visitor<'de> for FixedSizeDataVisitor<N> {
    type Value = FixedSizeData<N>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(&format!("hex string for {N} byte data"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let buf = decode_hex(value);

        Ok(Self::Value::from(buf.as_slice()))
    }
}

impl<'de, const N: usize> Deserialize<'de> for FixedSizeData<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(FixedSizeDataVisitor)
    }
}

impl<const N: usize> Serialize for FixedSizeData<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_hex(self.as_slice()))
    }
}

fn encode_hex(buf: &[u8]) -> String {
    format!("0x{}", faster_hex::hex_string(buf))
}

fn decode_hex(value: &str) -> Vec<u8> {
    let hex = value
        .strip_prefix("0x")
        .unwrap();

    let len = hex.as_bytes().len();
    let mut dst = vec![0; len / 2];

    faster_hex::hex_decode(hex.as_bytes(), &mut dst).unwrap();

    dst
}

impl<const N: usize> arrow_convert::field::ArrowField for FixedSizeData<N> {
    type Type = Self;

    fn data_type() -> DataType {
        return DataType::FixedSizeBinary(i32::try_from(N).unwrap())
    }
}

impl<const N: usize> arrow_convert::serialize::ArrowSerialize for FixedSizeData<N> {
    type ArrayBuilderType = builder::FixedSizeBinaryBuilder;

    fn new_array() -> Self::ArrayBuilderType {
        Self::ArrayBuilderType::new(i32::try_from(N).unwrap())
    }

    fn arrow_serialize(v: &Self, array: &mut Self::ArrayBuilderType) -> arrow::error::Result<()> {
        todo!()
    }
}

impl<const N: usize> arrow_convert::deserialize::ArrowDeserialize for FixedSizeData<N> {
    type ArrayType = FixedSizeBinaryArray;

    fn arrow_deserialize(v: <Self::ArrayType as ArrowArrayIterable>::Item<'_>) -> Option<Self> {
        todo!()
    }
}

