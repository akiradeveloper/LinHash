use super::*;

type ArchivedPage = <Page as rkyv::Archive>::Archived;

pub fn encode_page(page: &Page) -> Vec<u8> {
    rkyv::to_bytes::<rkyv::rancor::Error>(page)
        .unwrap()
        .to_vec()
}

pub fn decode_page(buf: &[u8]) -> Result<Page> {
    let page = rkyv::from_bytes::<Page, rkyv::rancor::Error>(buf)?;
    Ok(page)
}

pub struct PageRef {
    pub buf: PageIOBuffer,
    pub data_range: Range<usize>,
}

impl PageRef {
    #[inline]
    fn data(&self) -> &[u8] {
        &self.buf[self.data_range.clone()]
    }

    #[inline]
    fn archived(&self) -> &ArchivedPage {
        unsafe { rkyv::access_unchecked::<ArchivedPage>(self.data()) }
    }

    pub fn get_value(&self, key: &[u8]) -> Option<&[u8]> {
        let page = self.archived();
        page.kv_pairs.get(key).map(|v| v.as_slice())
    }

    pub fn overflow_id(&self) -> Option<u64> {
        self.archived().overflow_id.as_ref().map(|x| x.to_native())
    }

    pub fn locallevel(&self) -> Option<u8> {
        self.archived().locallevel.as_ref().map(|x| *x)
    }
}
