run

```
make run
```

to see the results.

Example result:

```
====================================================

[JSON]
	serialization:    12ms
	serialized size:  17477kB
	deserialization:  16ms
[JSON, GZIP]
	serialization:    238ms
	serialized size:  3711kB
	deserialization:  40ms
[JSON, ZSTD_1]
	serialization:    30ms
	serialized size:  2992kB
	deserialization:  22ms
[JSON, ZSTD_3]
	serialization:    51ms
	serialized size:  3108kB
	deserialization:  23ms
[JSON, ZSTD_9]
	serialization:    140ms
	serialized size:  3029kB
	deserialization:  26ms
---
[ARROW-IPC]
	serialization:    2ms
	serialized size:  8481kB
	deserialization:  2ms
[ARROW-IPC, ZSTD]
	serialization:    17ms
	serialized size:  2723kB
	deserialization:  4ms
[ARROW-IPC, light_compressed]
	serialization:    2ms
	serialized size:  3777kB
	deserialization:  4ms
---
[PARQUET]
	serialization:    5ms
	serialized size:  7527kB
	deserialization:  3ms
[PARQUET, ZSTD_1]
	serialization:    13ms
	serialized size:  2770kB
	deserialization:  5ms
[PARQUET, ZSTD_3]
	serialization:    18ms
	serialized size:  2653kB
	deserialization:  5ms
[PARQUET, ZSTD_9]
	serialization:    52ms
	serialized size:  2600kB
	deserialization:  6ms
---
[FLATBUFFERS]
	serialization:    3ms
	serialized size:  8847kB
	deserialization:  3ms
[FLATBUFFERS-CUSTOM, ZSTD_1]
	serialization:    15ms
	serialized size:  2998kB
	deserialization:  6ms
[FLATBUFFERS-CUSTOM, ZSTD_3]
	serialization:    23ms
	serialized size:  2735kB
	deserialization:  6ms
[FLATBUFFERS-CUSTOM, ZSTD_9]
	serialization:    62ms
	serialized size:  2586kB
	deserialization:  6ms

====================================================
```

