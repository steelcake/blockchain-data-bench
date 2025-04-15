run

```
RUSTFLAGS="-C target-cpu=native" cargo run --release
```

to see the results.

Example result:

```
[JSON]
	serialization:    16ms
	serialized size:  17477kB
	deserialization:  24ms
[JSON, GZIP]
	serialization:    211ms
	serialized size:  3711kB
	deserialization:  61ms
[JSON, ZSTD_1]
	serialization:    36ms
	serialized size:  2992kB
	deserialization:  32ms
[JSON, ZSTD_3]
	serialization:    57ms
	serialized size:  3108kB
	deserialization:  33ms
[JSON, ZSTD_9]
	serialization:    167ms
	serialized size:  3029kB
	deserialization:  37ms
---
[ARROW-IPC]
	serialization:    3ms
	serialized size:  8481kB
	deserialization:  2ms
[ARROW-IPC, ZSTD]
	serialization:    16ms
	serialized size:  2723kB
	deserialization:  4ms
---
[PARQUET]
	serialization:    6ms
	serialized size:  7527kB
	deserialization:  3ms
[PARQUET, ZSTD_1]
	serialization:    15ms
	serialized size:  2770kB
	deserialization:  6ms
[PARQUET, ZSTD_3]
	serialization:    18ms
	serialized size:  2653kB
	deserialization:  6ms
[PARQUET, ZSTD_9]
	serialization:    55ms
	serialized size:  2600kB
	deserialization:  6ms

====================================================
```
