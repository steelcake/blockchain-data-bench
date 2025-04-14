run 

```
RUSTFLAGS="-C target-cpu=native" cargo run --release
```

to see the results.

Example result:

```
took 17ms to serialize arrow_ipc
arrow ipc serialized size: 2723kB
took 4ms to deserialize arrow_ipc
took 237ms to serialize json
json serialized size: 3711kB
took 40ms to deserialize json
took 18ms to serialize parquet
parquet serialized size: 2653kB
took 6ms to deserialize parquet
```
