## Create a reader for each file and flat map rows
```rust
let rows = paths
.iter()
.map(|p| SerializedFileReader::try_from(*p).unwrap())
.flat_map(|r| r.into_iter());
```