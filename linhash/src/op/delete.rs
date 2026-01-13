use super::*;

pub struct Delete<'a> {
    pub db: &'a mut LinHash,
}

impl Delete<'_> {
    pub fn exec(self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let b = self.db.calc_main_page_id(key);

        let mut cur_page = (PageId::Main(b), self.db.main_pages.read_page(b)?.unwrap());

        loop {
            if cur_page.1.contains(key) {
                let removed = cur_page.1.kv_pairs.remove(key);
                match cur_page.0 {
                    PageId::Main(b) => self.db.main_pages.write_page_atomic(b, cur_page.1)?,
                    PageId::Overflow(id) => self.db.overflow_pages.write_page(id, cur_page.1)?,
                }

                if removed.is_some() {
                    self.db.n_items -= 1;
                }
                return Ok(removed);
            }

            if let Some(overflow_id) = cur_page.1.overflow_id {
                cur_page = (
                    PageId::Overflow(overflow_id),
                    self.db.overflow_pages.read_page(overflow_id)?.unwrap(),
                );
            } else {
                break;
            }
        }

        Ok(None)
    }
}
