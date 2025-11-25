run

```
make run
```

to see the results.

Example result:

```
######################### Benchmarking Dataset ETH ###############
[ArrowIPC]
        serialization:    45910us
        deserialization:    15198us
        serialized size:  7679kB
---
[Parquet NoCompression]
        serialization:    47379us
        deserialization:    21214us
        serialized size:  25887kB
[Parquet ZSTD<1>]
        serialization:    66124us
        deserialization:    27293us
        serialized size:  5739kB
[Parquet ZSTD<3>]
        serialization:    73447us
        deserialization:    27010us
        serialized size:  5537kB
---
[Olive]
        serialization:    24041us
        deserialization:    8025us
        serialized size:  9746kB
---

====================================================

######################### Benchmarking Dataset ETH_FIXED ###############
[ArrowIPC]
        serialization:    41131us
        deserialization:    14395us
        serialized size:  6502kB
---
[Parquet NoCompression]
        serialization:    48266us
        deserialization:    19237us
        serialized size:  35244kB
[Parquet ZSTD<1>]
        serialization:    69486us
        deserialization:    26995us
        serialized size:  5733kB
[Parquet ZSTD<3>]
        serialization:    76125us
        deserialization:    25324us
        serialized size:  5584kB
---
[Olive]
        serialization:    29459us
        deserialization:    7208us
        serialized size:  8018kB
---

====================================================

######################### Benchmarking Dataset SOLANA ###############
[ArrowIPC]
        serialization:    15048us
        deserialization:    5191us
        serialized size:  3144kB
---
[Parquet NoCompression]
        serialization:    16491us
        deserialization:    5632us
        serialized size:  3371kB
[Parquet ZSTD<1>]
        serialization:    21544us
        deserialization:    7267us
        serialized size:  2346kB
[Parquet ZSTD<3>]
        serialization:    25760us
        deserialization:    7198us
        serialized size:  2235kB
---
[Olive]
        serialization:    6952us
        deserialization:    2402us
        serialized size:  4014kB
---

====================================================

######################### Benchmarking Dataset SOLANA_FIXED ###############
[ArrowIPC]
        serialization:    12010us
        deserialization:    4324us
        serialized size:  2526kB
---
[Parquet NoCompression]
        serialization:    17116us
        deserialization:    4811us
        serialized size:  10357kB
[Parquet ZSTD<1>]
        serialization:    24490us
        deserialization:    6492us
        serialized size:  2398kB
[Parquet ZSTD<3>]
        serialization:    27635us
        deserialization:    6629us
        serialized size:  2280kB
---
[Olive]
        serialization:    7035us
        deserialization:    1828us
        serialized size:  2532kB
---

====================================================
```
