use super::*;

mod io;
use io::IO;

pub struct Device {
    io: IO,
}

impl Device {
    pub fn new(path: &Path) -> Result<Self> {
        Ok(Self { io: IO::new(path)? })
    }

    fn into_data(page: Page) -> Vec<u8> {
        let data = encode_page(&page);
        assert!(data.len() <= 4088);

        let crc = crc32fast::hash(&data);
        let data_len = data.len() as u32;

        let mut out = Vec::with_capacity(8 + data.len());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&data_len.to_le_bytes());
        out.extend_from_slice(&data);

        out
    }

    pub fn write_page(&self, id: u64, page: Page) -> Result<()> {
        let buf = Self::into_data(page);
        self.io.write(&buf, id * 4096)?;
        Ok(())
    }

    // We need to ensure writing to main pages is atomic but for now, it is not possible.
    // There is a risk of losing consistency if writing to main pages ended in torn write.
    pub fn write_page_atomic(&self, id: u64, page: Page) -> Result<()> {
        let buf = Self::into_data(page);
        self.io.write(&buf, id * 4096)?;
        Ok(())
    }

    pub fn read_page(&self, id: u64) -> Result<Option<Page>> {
        let mut buf = vec![0u8; 4096];
        self.io.read(&mut buf, id * 4096)?;

        let stored_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let data = &buf[8..(8 + data_len)];
        let calc_crc = crc32fast::hash(data);
        assert_eq!(stored_crc, calc_crc);

        match decode_page(data) {
            Ok(page) => Ok(Some(page)),
            Err(_) => Ok(None),
        }
    }

    pub fn read_page_ref(&self, id: u64) -> Result<Option<PageRef>> {
        let mut buf = AlignedVec::with_capacity(4096);
        buf.resize(4096, 0);

        self.io.read(&mut buf, id * 4096)?;

        let stored_crc = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        let data_range = 8..(8 + data_len);
        let calc_crc = crc32fast::hash(&buf[data_range.clone()]);
        assert_eq!(stored_crc, calc_crc);

        let page_ref = PageRef { buf, data_range };

        Ok(Some(page_ref))
    }

    pub fn flush(&self) -> Result<()> {
        self.io.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_page_ref() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let device = Device::new(f.path()).unwrap();

        let mut page = Page {
            kv_pairs: HashMap::new(),
            overflow_id: None,
        };
        page.insert(vec![1; 32], vec![1; 16]);
        page.insert(vec![2; 32], vec![2; 16]);

        device.write_page(3, page).unwrap();

        let page_ref = device.read_page_ref(3).unwrap().unwrap();
        assert_eq!(page_ref.get_value(&vec![1; 32]), Some(&vec![1; 16][..]));
        assert_eq!(page_ref.get_value(&vec![2; 32]), Some(&vec![2; 16][..]));
    }
}
