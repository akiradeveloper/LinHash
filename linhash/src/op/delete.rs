use super::*;

pub struct Delete<'a> {
    pub db: &'a LinHashCore,
    pub main_page_id: u64,
    #[allow(unused)]
    pub root: RwLockReadGuard<'a, Root>,
    #[allow(unused)]
    pub lock: util::ExclusiveLockGuard<'a>,
}

impl Delete<'_> {
    pub fn exec(self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let main_page_id = self.main_page_id;

        let mut cur_page = (
            PageId::Main(main_page_id),
            self.db.main_pages.read_page(main_page_id)?.unwrap(),
        );

        loop {
            if cur_page.1.contains(key) {
                let removed = cur_page.1.kv_pairs.remove(key);
                match cur_page.0 {
                    PageId::Main(b) => self.db.main_pages.write_page(b, cur_page.1)?,
                    PageId::Overflow(id) => self.db.overflow_pages.write_page(id, cur_page.1)?,
                }

                if removed.is_some() {
                    self.db.n_items.fetch_sub(1, Ordering::SeqCst);
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
