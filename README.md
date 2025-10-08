run

```
make run
```

to see the results.

Example result:

```
[JSON]
        serialization:    13ms
        serialized size:  17477kB
        deserialization:  11ms
[JSON, GZIP]
        serialization:    215ms
        serialized size:  3711kB
        deserialization:  33ms
[JSON, ZSTD_1]
        serialization:    28ms
        serialized size:  2992kB
        deserialization:  17ms
[JSON, ZSTD_3]
        serialization:    45ms
        serialized size:  3108kB
        deserialization:  18ms
[JSON, ZSTD_9]
        serialization:    124ms
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
        deserialization:  4ms
[PARQUET, ZSTD_3]
        serialization:    16ms
        serialized size:  2653kB
        deserialization:  5ms
[PARQUET, ZSTD_9]
        serialization:    46ms
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
        deserialization:  7ms
[FLATBUFFERS-CUSTOM, ZSTD_9]
        serialization:    54ms
        serialized size:  2586kB
        deserialization:  6ms
---
[OLIVE]
olive from_arrow in 1667us
olive write in 6781us
olive header write in 4us
olive schema write in 4us
        serialization:    11ms
        serialized size:  2784kB
olive schema read in 10us
olive header read in 3us
olive read in 2401us
olive to_arrow in 180us
olive read finish in 7us
        deserialization:  4ms

====================================================
```
