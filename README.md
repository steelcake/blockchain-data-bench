run

```
make run
```

to see the results.

Example result:

```
[Json]
        serialization:    17411us
        deserialization:    12819us
        serialized size:  17477kB
[Json Gzip]
        serialization:    110043us
        deserialization:    27351us
        serialized size:  3439kB
[Json ZSTD 1]
        serialization:    33264us
        deserialization:    19385us
        serialized size:  2992kB
[Json ZSTD 3]
        serialization:    50175us
        deserialization:    20194us
        serialized size:  3108kB
[Json ZSTD 9]
        serialization:    136614us
        deserialization:    27533us
        serialized size:  3029kB
---
[ArrowIPC]
        serialization:    15205us
        deserialization:    2821us
        serialized size:  2723kB
---
[Parquet]
        serialization:    4226us
        deserialization:    2038us
        serialized size:  7523kB
[Parquet ZSTD 1]
        serialization:    11637us
        deserialization:    3695us
        serialized size:  2767kB
[Parquet ZSTD 3]
        serialization:    15625us                                                                                                                        deserialization:    3513us
        serialized size:  2650kB
[Parquet ZSTD 9]
        serialization:    46165us
        deserialization:    3089us
        serialized size:  2596kB
---
[Flatbuffers]
        serialization:    5011us
        deserialization:    4001us
        serialized size:  8847kB
[Flatbuffers ZSTD 1]
        serialization:    15890us
        deserialization:    7347us
        serialized size:  2998kB
[Flatbuffers ZSTD 3]
        serialization:    22362us
        deserialization:    7603us
        serialized size:  2735kB
[Flatbuffers ZSTD 9]
        serialization:    57361us
        deserialization:    7280us
        serialized size:  2586kB
---
olive alloc in 263us
olive alloc in 290us
olive from_arrow in 2082us
olive write in 6257us
olive header write in 6us
olive schema write in 2us
olive schema read in 9us
olive header read in 3us
olive read in 2199us
olive to_arrow in 190us
olive read finish in 7us
[OLIVE]
        serialization:    8948us
        deserialization:    2452us
        serialized size:  2871kB
---
[VORTEX]
        serialization:    12541us
        deserialization:    4380us
        serialized size:  2762kB

====================================================
```
