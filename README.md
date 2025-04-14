run 

```
RUSTFLAGS="-C target-cpu=native" cargo run --release
```

to see the results.

Example result:

```
took 26ms to serialize arrow_ipc
arrow ipc serialized size: 8359kB
took 6ms to deserialize arrow_ipc
took 563ms to serialize json
json serialized size: 10098kB
took 89ms to deserialize json
```
