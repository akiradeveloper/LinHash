use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Range;
use std::path::Path;

mod error;
pub use error::Error;
use error::Result;

mod device;
use device::Device;
mod op;

mod page;
use page::*;

type PageIOBuffer = rkyv::util::AlignedVec<4096>;

#[derive(Clone, Copy)]
struct ReadLock(u64);

#[derive(Clone, Copy)]
struct SelectiveLock(u64);

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug)]
struct Page {
    kv_pairs: HashMap<Vec<u8>, Vec<u8>>,
    overflow_id: Option<u64>,
}

impl Page {
    fn new() -> Self {
        Self {
            kv_pairs: HashMap::new(),
            overflow_id: None,
        }
    }

    /// Return true if an existing key was replaced.
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Vec<u8>> {
        self.kv_pairs.insert(key, value)
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.kv_pairs.contains_key(key)
    }
}

fn calc_max_kv_per_page(ksize: usize, vsize: usize) -> u16 {
    let mut page = Page {
        kv_pairs: HashMap::new(),
        overflow_id: Some(1),
    };

    let mut n = 0;
    for i in 0..u16::MAX {
        let mut k = vec![0; ksize];
        let ibytes: [u8; 2] = i.to_le_bytes();
        for j in 0..std::cmp::min(2, ksize) {
            k[j] = ibytes[j];
        }

        let old = page.insert(k, vec![1; vsize]);
        if old.is_none() {
            n += 1
        }

        assert!(n >= 1);
        let buf = encode_page(&page);
        if buf.len() > 4064 {
            return n - 1;
        }
    }

    n
}

enum PageId {
    Main(u64),
    Overflow(u64),
}

struct Root {
    main_base_level: u8,
    next_split_main_page_id: u64,
}

impl Root {
    fn read_main_page(&self, hash: u64) -> ReadLock {
        let b = self.calc_main_page_id(hash);
        ReadLock(b)
    }

    fn write_main_page(&self, hash: u64) -> SelectiveLock {
        let b = self.calc_main_page_id(hash);
        SelectiveLock(b)
    }

    fn calc_n_pages(&self) -> u64 {
        (1 << self.main_base_level) + self.next_split_main_page_id
    }

    fn calc_main_page_id(&self, hash: u64) -> u64 {
        let b = hash & ((1 << self.main_base_level) - 1);
        if b < self.next_split_main_page_id {
            hash & ((1 << (self.main_base_level + 1)) - 1)
        } else {
            b
        }
    }

    // Only this function updates `next_split_main_page_id` and `main_base_level`.
    fn advance_split_pointer(&mut self) {
        self.next_split_main_page_id += 1;
        if self.next_split_main_page_id == (1 << self.main_base_level) {
            self.main_base_level += 1;
            self.next_split_main_page_id = 0;
        }
    }
}

pub struct LinHash {
    main_pages: Device,

    root: Root,

    overflow_pages: Device,
    next_overflow_id: u64,

    n_items: u64,
    max_kv_per_page: u16,
}

impl LinHash {
    fn new(dir: &Path, ksize: usize, vsize: usize) -> Result<Self> {
        let main_pages = Device::new(&dir.join("main"))?;
        let overflow_pages = Device::new(&dir.join("overflow"))?;

        Ok(Self {
            main_pages,
            root: Root {
                main_base_level: 1,
                next_split_main_page_id: 0,
            },

            overflow_pages,
            next_overflow_id: 0,

            max_kv_per_page: calc_max_kv_per_page(ksize, vsize),
            n_items: 0,
        })
    }

    pub fn open(dir: &Path, ksize: usize, vsize: usize) -> Result<Self> {
        let mut db = Self::new(dir, ksize, vsize)?;

        let n_main_pages = op::Restore { db: &mut db }.exec()?;

        // Invariant: there are at least two valid main pages.
        if n_main_pages < 2 {
            op::Init { db: &mut db }.exec()?;
            op::Restore { db: &mut db }.exec()?;
        }

        Ok(db)
    }

    // The key must be at least 64 bits.
    #[cfg(not(feature = "hash"))]
    fn calc_hash(&self, key: &[u8]) -> u64 {
        let a: [u8; 8] = key[0..8].try_into().ok().unwrap();
        u64::from_le_bytes(a)
    }

    #[cfg(feature = "hash")]
    fn calc_hash(&self, key: &[u8]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(key)
    }

    fn load_factor(&self) -> f64 {
        let n_main_pages = self.root.calc_n_pages();
        let max_items = n_main_pages * self.max_kv_per_page as u64;
        self.n_items as f64 / max_items as f64
    }

    pub fn len(&self) -> u64 {
        self.n_items
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let lock = self.root.read_main_page(self.calc_hash(key));
        op::Get { db: self, lock }.exec(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let lock = self.root.write_main_page(self.calc_hash(&key));
        let old = op::Insert { db: self, lock }.exec(key, value)?;

        if self.load_factor() > 0.8 {
            let lock = self.root.read_main_page(self.root.next_split_main_page_id);
            let page_chains = op::SplitPrepare { db: self, lock }.exec();

            if let Ok(page_chains) = page_chains {
                op::SplitCommit { db: self }.exec(page_chains).ok();
            }
        }

        Ok(old)
    }

    #[cfg(feature = "delete")]
    pub fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let lock = self.root.write_main_page(self.calc_hash(key));
        op::Delete { db: self, lock }.exec(key)
    }
}
