# LinHash

[![Crates.io](https://img.shields.io/crates/v/linhash.svg)](https://crates.io/crates/linhash)
[![API doc](https://docs.rs/linhash/badge.svg)](https://docs.rs/linhash)
![CI](https://github.com/akiradeveloper/linhash/actions/workflows/ci-master.yml/badge.svg)

Linear Hashing implementation in Rust.

## What's good about Linear Hashing?

- It is a on-disk data structure to maintain a key-value mapping.
- It doesn't use much RAM except temporary buffers.
- Since queries need only one or two reads from the disk, it is very fast.
- The query performance doesn't depend on the database size.
- Concurrency is researched. → [Concurrency in linear hashing (Paper)](https://dl.acm.org/doi/10.1145/22952.22954)
- The algorithm is simple and elegant.

## What's good about this implementation?

- Use rkyv's zero-copy deserialization for fast queries.
- Use RWF_ATOMIC flag for avoiding torn writes.

## Limitations

- Key size and value size must be fixed.

## Example

The API is as same as `HashMap`.

```rust
use linhash::LinHash;

let dir = tempfile::tempdir().unwrap();
let ksize = 2;
let vsize = 4;
let mut db = LinHash::open(dir.path(), ksize, vsize).unwrap();

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
| hash | on | If disabled, 64 bit from the given key is taken as a hash, eliminating the cost of hashing. |
| delete | on | Enable deletion. Disable deletion optimize insertion because holed pages can't exist. |