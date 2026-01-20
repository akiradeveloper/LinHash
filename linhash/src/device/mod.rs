use super::*;

mod io;
pub use io::IO;

const MAGIC: u32 = 0x4c6e4861; // LnHa
const HEADER_LEN: usize = 32;

pub struct Device {
    io: IO,
    pagesize: usize,
}

impl Device {
    pub fn open(path: &Path, pagesize: usize) -> Result<Self> {
        Ok(Self {
            io: IO::open(path)?,
            pagesize,
        })
    }

    fn into_data(&self, page: &Page) -> PageIOBuffer {
        let data = encode_page(&page);
        assert!(data.len() <= self.pagesize - HEADER_LEN);

        let crc = crc32fast::hash(&data);
        let data_len = data.len() as u32;

        let mut out = PageIOBuffer::with_capacity(self.pagesize);
        out.extend_from_slice(&MAGIC.to_le_bytes()); // 4
        out.extend_from_slice(&crc.to_le_bytes()); // 4
        out.extend_from_slice(&data_len.to_le_bytes()); // 4
        out.extend_from_slice(&[0; HEADER_LEN - 12]); // Padding
        out.extend_from_slice(&data);
        out.resize(self.pagesize, 0);

        out
    }

    pub fn write_page(&self, id: u64, page: &Page) -> Result<()> {
        let buf = self.into_data(page);
        self.io.write(&buf, id * self.pagesize as u64)?;
        Ok(())
    }

    pub fn read_page(&self, id: u64) -> Result<Option<Page>> {
        let mut buf = PageIOBuffer::with_capacity(self.pagesize);
        buf.resize(self.pagesize, 0);
        self.io.read(&mut buf, id * self.pagesize as u64)?;

        let stored_magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if stored_magic != MAGIC {
            return Ok(None);
        }

        let stored_crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;

        let data = &buf[HEADER_LEN..(HEADER_LEN + data_len)];
        let calc_crc = crc32fast::hash(data);
        if stored_crc != calc_crc {
            return Ok(None);
        }

        match decode_page(data) {
            Ok(page) => Ok(Some(page)),
            Err(_) => Ok(None),
        }
    }

    pub fn read_page_ref(&self, id: u64) -> Result<Option<PageRef>> {
        let mut buf = PageIOBuffer::with_capacity(self.pagesize);
        buf.resize(self.pagesize, 0);

        self.io.read(&mut buf, id * self.pagesize as u64)?;

        let stored_magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if stored_magic != MAGIC {
            return Ok(None);
        }

        let stored_crc = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;

        let data_range = HEADER_LEN..(HEADER_LEN + data_len);
        let calc_crc = crc32fast::hash(&buf[data_range.clone()]);
        if stored_crc != calc_crc {
            return Ok(None);
        }

        let page_ref = PageRef { buf, data_range };

        Ok(Some(page_ref))
    }

    pub fn flush(&self) -> Result<()> {
        self.io.flush()?;
        Ok(())
    }

    /// Free the storage blocks of pages in [start, end).
    pub fn free_page_range(&self, start: u64, end: u64) -> Result<()> {
        let n_pages = end - start;
        self.io
            .free(start * self.pagesize as u64, n_pages * self.pagesize as u64)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_read_page() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let device = Device::open(f.path(), 8192).unwrap();

        let mut page = Page {
            kv_pairs: HashMap::new(),
            overflow_id: None,
            locallevel: None,
        };
        page.insert(vec![1; 32], vec![1; 16]);
        page.insert(vec![2; 32], vec![2; 16]);

        device.write_page(3, &page).unwrap();

        let read_page = device.read_page(3).unwrap().unwrap();
        assert_eq!(read_page.kv_pairs.get(&vec![1; 32]), Some(&vec![1; 16]));
        assert_eq!(read_page.kv_pairs.get(&vec![2; 32]), Some(&vec![2; 16]));
    }

    #[test]
    fn test_read_page_ref() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let device = Device::open(f.path(), 8192).unwrap();

        let mut page = Page {
            kv_pairs: HashMap::new(),
            overflow_id: None,
            locallevel: None,
        };
        page.insert(vec![1; 32], vec![1; 16]);
        page.insert(vec![2; 32], vec![2; 16]);

        device.write_page(3, &page).unwrap();

        let page_ref = device.read_page_ref(3).unwrap().unwrap();
        assert_eq!(page_ref.get_value(&vec![1; 32]), Some(&vec![1; 16][..]));
        assert_eq!(page_ref.get_value(&vec![2; 32]), Some(&vec![2; 16][..]));
    }
}
