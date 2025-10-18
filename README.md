run

```
make run
```

to see the results.

Example result:

```
[Json]
        serialization:    18593us
        deserialization:    11783us
        serialized size:  17477kB
[Json Gzip]
        serialization:    216783us
        deserialization:    34257us
        serialized size:  3711kB
[Json ZSTD 1]
        serialization:    32914us
        deserialization:    17844us
        serialized size:  2992kB
[Json ZSTD 3]
        serialization:    49133us
        deserialization:    20319us
        serialized size:  3108kB
[Json ZSTD 9]
        serialization:    127797us
        deserialization:    23843us
        serialized size:  3029kB
---
[ArrowIPC]
        serialization:    14093us
        deserialization:    2742us
        serialized size:  2723kB
---
[Parquet]
        serialization:    4243us
        deserialization:    1599us
        serialized size:  7527kB
[Parquet ZSTD 1]
        serialization:    11102us
        deserialization:    3351us
        serialized size:  2770kB
[Parquet ZSTD 3]
        serialization:    14971us
        deserialization:    3203us
        serialized size:  2653kB
[Parquet ZSTD 9]
        serialization:    45356us
        deserialization:    3163us
        serialized size:  2600kB
---
[Flatbuffers]
        serialization:    4961us
        deserialization:    4309us
        serialized size:  8847kB
[Flatbuffers ZSTD 1]
        serialization:    15218us
        deserialization:    7714us
        serialized size:  2998kB
[Flatbuffers ZSTD 3]
        serialization:    22263us
        deserialization:    7388us
        serialized size:  2735kB
[Flatbuffers ZSTD 9]
        serialization:    55344us
        deserialization:    7460us
        serialized size:  2586kB
---
olive alloc in 219us
olive alloc in 251us
olive from_arrow in 1633us
olive write in 6113us
olive header write in 4us
olive schema write in 2us
olive schema read in 7us
olive header read in 3us
olive read in 2058us
olive to_arrow in 193us
olive read finish in 6us
[OLIVE]
        serialization:    8267us
        deserialization:    2304us
        serialized size:  2871kB

====================================================
```
