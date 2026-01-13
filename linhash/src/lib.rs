use rkyv::util::AlignedVec;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::Path;

mod error;
pub use error::Error;
use error::Result;

mod device;
use device::Device;
mod op;

mod page;
use page::*;

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

fn calc_max_kv_per_page(ksize: usize, vsize: usize) -> u8 {
    for i in 0..=255 {
        let mut page = Page {
            kv_pairs: HashMap::new(),
            overflow_id: Some(1),
        };
        for j in 0..i {
            page.insert(vec![j; ksize], vec![j; vsize]);
        }

        let buf = encode_page(&page);
        if buf.len() > 4088 {
            assert!(i > 2);
            return i - 1;
        }
    }

    255
}

enum PageId {
    Main(u64),
    Overflow(u64),
}

pub struct LinHash {
    main_pages: Device,
    main_base_level: u8,
    next_split_main_page_id: u64,

    overflow_pages: Device,
    next_overflow_id: u64,

    n_items: u64,
    max_kv_per_page: Option<u8>,
}

impl LinHash {
    pub fn new(dir: &Path) -> Result<Self> {
        let main_pages = Device::new(&dir.join("main"))?;
        let overflow_pages = Device::new(&dir.join("overflow"))?;

        Ok(Self {
            main_pages,
            main_base_level: 1,
            next_split_main_page_id: 0,

            overflow_pages,
            next_overflow_id: 0,

            max_kv_per_page: None,
            n_items: 0,
        })
    }

    pub fn open(dir: &Path) -> Result<Self> {
        let mut db = Self::new(dir)?;

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
    fn hash_key(&self, key: &[u8]) -> u64 {
        let a: [u8; 8] = key[0..8].try_into().ok().unwrap();
        u64::from_le_bytes(a)
    }

    #[cfg(feature = "hash")]
    fn hash_key(&self, key: &[u8]) -> u64 {
        xxhash_rust::xxh3::xxh3_64(key)
    }

    fn calc_main_page_id(&self, key: &[u8]) -> u64 {
        let hash = self.hash_key(key);

        let b = hash & ((1 << self.main_base_level) - 1);
        if b < self.next_split_main_page_id {
            hash & ((1 << (self.main_base_level + 1)) - 1)
        } else {
            b
        }
    }

    fn load_factor(&self) -> f64 {
        let n_main_pages = (1 << self.main_base_level) + self.next_split_main_page_id;
        let max_items = n_main_pages * self.max_kv_per_page.unwrap() as u64;
        self.n_items as f64 / max_items as f64
    }

    pub fn len(&self) -> u64 {
        self.n_items
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        op::Get { db: self }.exec(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // If the existence of the key is confirmed, insertion will be updating.
        let confirmed = op::Get { db: self }.exec(&key)?.is_some();

        let old = op::Insert { db: self }.exec(key, value, confirmed)?;

        if self.load_factor() > 0.8 {
            op::Split { db: self }.exec().ok();
        }

        Ok(old)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        op::Delete { db: self }.exec(key)
    }
}
