run

```
make run
```

to see the results.

Example result:

```
[JSON]
        serialization:    14ms
        serialized size:  17477kB
        deserialization:  11ms
[JSON, GZIP]
        serialization:    217ms
        serialized size:  3711kB
        deserialization:  34ms
[JSON, ZSTD_1]
        serialization:    30ms
        serialized size:  2992kB
        deserialization:  17ms
[JSON, ZSTD_3]
        serialization:    46ms
        serialized size:  3108kB
        deserialization:  19ms
[JSON, ZSTD_9]
        serialization:    125ms
        serialized size:  3029kB
        deserialization:  21ms
---
[ARROW-IPC]
        serialization:    2ms
        serialized size:  8481kB
        deserialization:  1ms
[ARROW-IPC, ZSTD]
        serialization:    14ms
        serialized size:  2723kB
        deserialization:  3ms
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
        serialization:    12ms
        serialized size:  2770kB
        deserialization:  5ms
[PARQUET, ZSTD_3]
        serialization:    16ms
        serialized size:  2653kB
        deserialization:  5ms
[PARQUET, ZSTD_9]
        serialization:    45ms
        serialized size:  2600kB
        deserialization:  4ms
---
[FLATBUFFERS]
        serialization:    3ms
        serialized size:  8847kB
        deserialization:  3ms
[FLATBUFFERS-CUSTOM, ZSTD_1]
        serialization:    13ms
        serialized size:  2998kB
        deserialization:  6ms
[FLATBUFFERS-CUSTOM, ZSTD_3]
        serialization:    20ms
        serialized size:  2735kB
        deserialization:  6ms
[FLATBUFFERS-CUSTOM, ZSTD_9]
        serialization:    53ms
        serialized size:  2586kB
        deserialization:  6ms
---
olive write IN 7ms
[OLIVE]
        serialization:    216ms
        serialized size:  2790kB
olive read IN 2ms
        deserialization:  5ms
====================================================
```

