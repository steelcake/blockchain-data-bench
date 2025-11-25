use arrow::array::{Array, AsArray, StructArray, make_array, make_comparator};
use arrow::datatypes::{DataType, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vortex::Array as VortexArray;
use vortex::arrow::{FromArrowArray, IntoArrowArray};
use vortex::compressor::CompactCompressor;
use vortex::file::{VortexOpenOptions, VortexWriteOptions, WriteStrategyBuilder};
use vortex::io::runtime::single::SingleThreadRuntime;

use cherry_core::ingest;
use ingest::evm;

use futures_lite::StreamExt;

use tikv_jemallocator::Jemalloc;
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    let eth_data = fetch_eth_data().await;
    let eth_schema = eth_data
        .iter()
        .map(|(name, table)| (name.clone(), table.schema()))
        .collect::<BTreeMap<String, Arc<Schema>>>();

    bench_one::<ArrowIPC>(&ArrowIPC, &eth_data).print();
    println!("---");

    bench_one::<Parquet>(&Parquet { zstd_level: None }, &eth_data).print();
    bench_one::<Parquet>(
        &Parquet {
            zstd_level: Some(1),
        },
        &eth_data,
    )
    .print();
    bench_one::<Parquet>(
        &Parquet {
            zstd_level: Some(3),
        },
        &eth_data,
    )
    .print();
    println!("---");

    bench_one::<Vortex>(
        &Vortex {
            schema: eth_schema.clone(),
        },
        &eth_data,
    )
    .print();
    println!("---");

    bench_one::<Olive>(
        &Olive {
            ctx: OliveContext::new(),
        },
        &eth_data,
    )
    .print();
    println!("---");

    println!("\n====================================================\n");
}

async fn fetch_eth_data() -> BTreeMap<String, RecordBatch> {
    let query = ingest::Query::Evm(evm::Query {
        from_block: 20_123_123,
        to_block: Some(20_123_173),
        include_all_blocks: true,
        transactions: vec![evm::TransactionRequest::default()],
        logs: vec![evm::LogRequest::default()],
        traces: vec![evm::TraceRequest::default()],
        fields: evm::Fields::all(),
        ..Default::default()
    });

    let mut conf = ingest::ProviderConfig::new(ingest::ProviderKind::Sqd);
    conf.url = Some("https://portal.sqd.dev/datasets/ethereum-mainnet".to_owned());

    let mut stream = ingest::start_stream(conf, query).await.unwrap();

    let mut data = BTreeMap::<String, Vec<RecordBatch>>::new();

    while let Some(res) = stream.next().await {
        let res = res.unwrap();

        for table_name in res.keys() {
            if !data.contains_key(table_name) {
                data.insert(table_name.clone(), Vec::new());
            }
        }

        for (table_name, table) in res.into_iter() {
            data.get_mut(&table_name).unwrap().push(table);
        }
    }

    let mut out = BTreeMap::<String, RecordBatch>::new();

    for (table_name, chunks) in data.into_iter() {
        out.insert(
            table_name,
            arrow::compute::concat_batches(&chunks[0].schema(), &chunks).unwrap(),
        );
    }

    out
}

struct BenchResult {
    serialization: Duration,
    deserialization: Duration,
    size: usize,
    format_name: String,
}

impl BenchResult {
    pub fn print(&self) {
        println!("[{}]", self.format_name);
        println!("\tserialization:    {}us", self.serialization.as_micros());
        println!(
            "\tdeserialization:    {}us",
            self.deserialization.as_micros()
        );
        println!("\tserialized size:  {}kB", self.size / (1 << 10));
    }
}

trait FileFormat {
    type Serialized;

    fn serialize(&self, data: &BTreeMap<String, RecordBatch>) -> Self::Serialized;
    fn deserialize(&self, data: Self::Serialized) -> BTreeMap<String, RecordBatch>;
    fn total_len(data: &Self::Serialized) -> usize;
    fn name(&self) -> String;
}

struct Olive {
    ctx: OliveContext,
}

impl FileFormat for Olive {
    type Serialized = &'static [u8];

    fn serialize(&self, data: &BTreeMap<String, RecordBatch>) -> Self::Serialized {
        to_olive(&self.ctx, &data)
    }

    fn deserialize(&self, data: Self::Serialized) -> BTreeMap<String, RecordBatch> {
        from_olive(&self.ctx, data)
    }

    fn total_len(data: &Self::Serialized) -> usize {
        data.len()
    }

    fn name(&self) -> String {
        "Olive".to_owned()
    }
}

struct Parquet {
    zstd_level: Option<i32>,
}

impl FileFormat for Parquet {
    type Serialized = BTreeMap<String, Vec<u8>>;

    fn serialize(&self, data: &BTreeMap<String, RecordBatch>) -> Self::Serialized {
        data.iter()
            .map(|(name, table)| (name.clone(), to_parquet(table, self.zstd_level)))
            .collect::<BTreeMap<String, Vec<u8>>>()
    }

    fn deserialize(&self, data: Self::Serialized) -> BTreeMap<String, RecordBatch> {
        data.into_iter()
            .map(|(name, table)| (name, from_parquet(table)))
            .collect()
    }

    fn total_len(data: &Self::Serialized) -> usize {
        data.values().map(|v| v.len()).sum()
    }

    fn name(&self) -> String {
        if let Some(level) = self.zstd_level {
            format!("Parquet ZSTD<{}>", level)
        } else {
            "Parquet NoCompression".to_owned()
        }
    }
}

struct Vortex {
    schema: BTreeMap<String, Arc<Schema>>,
}

impl FileFormat for Vortex {
    type Serialized = BTreeMap<String, Vec<u8>>;

    fn serialize(&self, data: &BTreeMap<String, RecordBatch>) -> Self::Serialized {
        data.iter()
            .map(|(name, table)| (name.clone(), to_vortex(table)))
            .collect::<BTreeMap<String, Vec<u8>>>()
    }

    fn deserialize(&self, data: Self::Serialized) -> BTreeMap<String, RecordBatch> {
        data.into_iter()
            .map(|(name, table)| {
                let schema = self.schema.get(&name).unwrap();
                (name, from_vortex(schema, table))
            })
            .collect()
    }

    fn total_len(data: &Self::Serialized) -> usize {
        data.values().map(|v| v.len()).sum()
    }

    fn name(&self) -> String {
        "Vortex".to_owned()
    }
}

struct ArrowIPC;

impl FileFormat for ArrowIPC {
    type Serialized = BTreeMap<String, Vec<u8>>;

    fn serialize(&self, data: &BTreeMap<String, RecordBatch>) -> Self::Serialized {
        data.iter()
            .map(|(name, table)| (name.clone(), to_arrow_ipc(table)))
            .collect::<BTreeMap<String, Vec<u8>>>()
    }

    fn deserialize(&self, data: Self::Serialized) -> BTreeMap<String, RecordBatch> {
        data.into_iter()
            .map(|(name, table)| (name, from_arrow_ipc(&table)))
            .collect()
    }

    fn total_len(data: &Self::Serialized) -> usize {
        data.values().map(|v| v.len()).sum()
    }

    fn name(&self) -> String {
        "ArrowIPC".to_owned()
    }
}

fn check_eq(
    format_name: &str,
    expected: &BTreeMap<String, RecordBatch>,
    got: &BTreeMap<String, RecordBatch>,
) {
    assert_eq!(expected.len(), got.len());

    for (table_name, expected_table) in expected.iter() {
        let got_table = got.get(table_name).unwrap();

        let expected_array = StructArray::from(expected_table.clone());
        let got_array = StructArray::from(got_table.clone());

        assert_eq!(
            expected_array.len(),
            got_array.len(),
            "failed len equality at: format={};table_name={};",
            format_name,
            table_name
        );

        let comparator = make_comparator(&expected_array, &got_array, Default::default()).unwrap();

        for idx in 0..expected_array.len() {
            if comparator(idx, idx) != Ordering::Equal {
                panic!(
                    "failed check_eq at: format={};table_name={};",
                    format_name, table_name
                );
            }
        }
    }
}

fn bench_one<F: FileFormat>(format: &F, data: &BTreeMap<String, RecordBatch>) -> BenchResult {
    let start = Instant::now();
    let serialized = format.serialize(data);
    let serialization = start.elapsed();
    let size = F::total_len(&serialized);

    let start = Instant::now();
    let deserialized = format.deserialize(serialized);
    let deserialization = start.elapsed();

    let format_name = format.name();

    check_eq(&format_name, data, &deserialized);

    BenchResult {
        format_name,
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

#[repr(C)]
struct SerializeOutput {
    len: u32,
    ptr: *mut u8,
}

#[repr(C)]
struct OliveContext {
    serialize_mem: *mut u8,
    deserialize_mem: *mut u8,
    out_mem: *mut u8,
}

impl OliveContext {
    fn new() -> Self {
        unsafe { olivers_init_ctx() }
    }
}

impl Drop for OliveContext {
    fn drop(&mut self) {
        unsafe {
            olivers_deinit_ctx(self);
        }
    }
}

#[link(name = "olivers")]
unsafe extern "C" {
    fn olivers_init_ctx() -> OliveContext;

    fn olivers_deinit_ctx(ctx: &mut OliveContext);

    fn olivers_ffi_serialize(
        ctx: &OliveContext,
        table_names_raw: *const *const u8,
        arrays: *const FFI_ArrowArray,
        schemas: *const FFI_ArrowSchema,
        n_tables: usize,
    ) -> SerializeOutput;

    fn olivers_ffi_deserialize(
        ctx: &OliveContext,
        data: SerializeOutput,
        table_names_out: *mut *const *const u8,
        arrays_out: *mut *const FFI_ArrowArray,
        schemas_out: *mut *const FFI_ArrowSchema,
        n_tables: *mut usize,
    );
}

fn to_olive(ctx: &OliveContext, data: &BTreeMap<String, RecordBatch>) -> &'static [u8] {
    let mut table_names = Vec::<*const u8>::with_capacity(data.len());
    let mut ffi_arrays = Vec::<FFI_ArrowArray>::with_capacity(data.len());
    let mut ffi_schemas = Vec::<FFI_ArrowSchema>::with_capacity(data.len());

    for (name, batch) in data.iter() {
        let arr_data = StructArray::from(batch.clone()).into_data();
        let (ffi_arr, ffi_schema) = arrow::ffi::to_ffi(&arr_data).unwrap();

        let c_name = CString::new(name.clone()).unwrap();

        table_names.push(c_name.as_ptr() as *const u8);
        ffi_arrays.push(ffi_arr);
        ffi_schemas.push(ffi_schema);

        std::mem::forget(c_name);
    }

    let out = unsafe {
        olivers_ffi_serialize(
            ctx,
            table_names.as_ptr(),
            ffi_arrays.as_ptr(),
            ffi_schemas.as_ptr(),
            data.len(),
        )
    };

    let out = unsafe { std::slice::from_raw_parts(out.ptr, usize::try_from(out.len).unwrap()) };

    std::mem::forget(ffi_arrays);
    std::mem::forget(ffi_schemas);
    std::mem::forget(table_names);

    out
}

fn from_olive(ctx: &OliveContext, data: &'static [u8]) -> BTreeMap<String, RecordBatch> {
    let mut table_names_out: *const *const u8 = std::ptr::null();
    let mut arrays_out: *const FFI_ArrowArray = std::ptr::null();
    let mut schemas_out: *const FFI_ArrowSchema = std::ptr::null();
    let mut n_tables: usize = 0;

    unsafe {
        olivers_ffi_deserialize(
            ctx,
            SerializeOutput {
                ptr: data.as_ptr() as *mut _,
                len: u32::try_from(data.len()).unwrap(),
            },
            &mut table_names_out,
            &mut arrays_out,
            &mut schemas_out,
            &mut n_tables,
        );
    }

    let mut out = BTreeMap::<String, RecordBatch>::new();

    unsafe {
        for idx in 0..n_tables {
            let arr = Box::from_raw(arrays_out.add(idx) as *mut FFI_ArrowArray);
            let arr_data = arrow::ffi::from_ffi(*arr, &*schemas_out.add(idx)).unwrap();
            let arr = make_array(arr_data);

            let name = CStr::from_ptr(*table_names_out.add(idx) as *const i8)
                .to_str()
                .unwrap()
                .to_owned();
            let batch = RecordBatch::try_from(arr.as_struct()).unwrap();

            out.insert(name, batch);
        }
    }

    out
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

fn from_vortex(schema: &Schema, data: Vec<u8>) -> RecordBatch {
    let res: Vec<Arc<dyn VortexArray>> = VortexOpenOptions::default()
        .open_buffer(data)
        .unwrap()
        .scan()
        .unwrap()
        .into_array_iter(&SingleThreadRuntime::default())
        .unwrap()
        .map(|x| x.unwrap())
        .collect();

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
