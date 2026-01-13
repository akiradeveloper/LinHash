# LinHash

Linear Hashing implementation in Rust.

## Features

| flag | default | description |
| -- | -- | -- |
| hash | off | By default, 64 bit part from give key is used as hash to eliminate the cost of hashing. By enabling this, hashing is used so it can accept any keys. |