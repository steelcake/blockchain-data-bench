run

```
make run
```

to see the results.

Example result:

```
[Json]
        serialization:    18951us
        deserialization:    12761us
        serialized size:  17477kB
[Json Gzip]
        serialization:    218223us
        deserialization:    34053us
        serialized size:  3711kB
[Json ZSTD 1]
        serialization:    32992us
        deserialization:    17663us
        serialized size:  2992kB
[Json ZSTD 3]
        serialization:    51071us
        deserialization:    20831us
        serialized size:  3108kB
[Json ZSTD 9]
        serialization:    132484us
        deserialization:    23968us
        serialized size:  3029kB
---
[ArrowIPC]
        serialization:    14419us
        deserialization:    3032us
        serialized size:  2723kB
---
[Parquet]
        serialization:    4373us
        deserialization:    1593us
        serialized size:  7527kB
[Parquet ZSTD 1]
        serialization:    11319us
        deserialization:    3440us
        serialized size:  2770kB
[Parquet ZSTD 3]
        serialization:    15343us
        deserialization:    3380us
        serialized size:  2653kB
[Parquet ZSTD 9]
        serialization:    45853us
        deserialization:    3229us
        serialized size:  2600kB
---
[Flatbuffers]
        serialization:    4866us
        deserialization:    4521us
        serialized size:  8847kB
[Flatbuffers ZSTD 1]
        serialization:    16537us
        deserialization:    7448us
        serialized size:  2998kB
[Flatbuffers ZSTD 3]
        serialization:    22870us
        deserialization:    7629us
        serialized size:  2735kB
[Flatbuffers ZSTD 9]
        serialization:    57468us
        deserialization:    8162us
        serialized size:  2586kB
---
olive from_arrow in 1619us
olive write in 6286us
olive header write in 6us
olive schema write in 2us
olive schema read in 9us
olive header read in 3us
olive read in 2266us
olive to_arrow in 201us
olive read finish in 7us
[OLIVE]
        serialization:    10167us
        deserialization:    2579us
        serialized size:  2871kB

====================================================
```
