# LinHash

Linear Hashing implementation in Rust.

## What's good about Linear Hashing?

- It is a on-disk data structure to maintain a key-value mapping.
- It doesn't use RAM except temporary buffers.
- Since queries need only one or two reads from the disk, it is very fast.
- The query performance doesn't depend on the database size.
- The algorithm is simple and elegant.

## Feature Gates

| flag | default | description |
| -- | -- | -- |
| hash | on | If disabled, 64 bit from the given key is taken as a hash, eliminating the cost of hashing. |