# LinHash

[![Crates.io](https://img.shields.io/crates/v/linhash.svg)](https://crates.io/crates/linhash)
[![API doc](https://docs.rs/linhash/badge.svg)](https://docs.rs/linhash)
![CI](https://github.com/akiradeveloper/linhash/actions/workflows/ci-master.yml/badge.svg)

Concurrent Linear Hashing implementation in Rust.

Best suit for metadata store for a database.

## What's good about Linear Hashing?

- It is a on-disk data structure to maintain key-value mappings.
- It doesn't use much RAM except temporary buffers.
- Since GETs need only one or two reads from the disk, it is very fast.
- The GET performance isn't affected by the database size.
- Concurrency is well studied in [Concurrency in linear hashing](https://dl.acm.org/doi/10.1145/22952.22954).
- The algorithm is simple and elegant.

## What's good about this implementation?

- GETs are never blocked by other operations except LIST.
- GETs and INSERTs are fully concurrent.
- Use rkyv's zero-copy deserialization for fast queries.
- Use RWF_ATOMIC flag for avoiding torn writes.

## Type-safe concurrency

Each operation is designed to take a hierarchy of locks before doing its work.
This is **type-checked** by Rust compiler.

| | Read Lock | Selective Lock | Exclusive Lock |
| -- | -- | -- | -- |
| Read Lock | ✅️ | ✅️ | ❌ |
| Selective Lock | ✅️ | ❌️ | ❌️ |
| Exclusive Lock | ❌️ | ❌️ | ❌️ |

| Operation | Root Lock | Bucket Lock |
| -- | -- | -- |
| INSERT | Read Lock | Selective Lock |
| DELETE | Read Lock | Exclusive Lock |
| GET | Read Lock | Read Lock |
| LIST | Exclusive Lock | |
| SPLIT | Read Lock | Selective Lock |

## Limitations

- Key size and value size must be fixed.

## Example

The API is as same as `HashMap`.

```rust
use linhash::LinHash;

let dir = tempfile::tempdir().unwrap();
let ksize = 2;
let vsize = 4;
let db = LinHash::open(dir.path(), ksize, vsize).unwrap();

db.insert(vec![1, 2], vec![3, 4, 5, 6]).unwrap();
let old = db.insert(vec![1, 2], vec![7, 8, 9, 10]).unwrap();
assert_eq!(old, Some(vec![3, 4, 5, 6]));

assert_eq!(db.get(&vec![1, 2]).unwrap(), Some(vec![7, 8, 9, 10]));

let old = db.delete(&vec![1, 2]).unwrap();
assert_eq!(old, Some(vec![7, 8, 9, 10]));
assert_eq!(db.get(&vec![1, 2]).unwrap(), None);
```

## Cargo Features

| flag | default | description |
| -- | -- | -- |
| hash | on | Enabled → Hash function is used to calculate hash from key. Disabled → 64 bit from the given key is taken as a hash, eliminating the cost of hashing. |
| atomic-write | off | Enabled → writes are atomic using RWF_ATOMIC. Disabled → crash tolerance is not guaranteed but high speed. |