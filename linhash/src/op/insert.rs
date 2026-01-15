use super::*;

pub struct Insert<'a> {
    pub db: &'a LinHashCore,
    pub main_page_id: u64,
    pub root: RwLockReadGuard<'a, Root>,
    pub lock: util::SelectiveLockGuard<'a>,
}

impl Insert<'_> {
    pub fn exec(self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let main_page_id = self.main_page_id;

        // If the existence of the key is confirmed, insertion will be updating.
        #[cfg(feature = "delete")]
        let replace_found = op::Get {
            db: self.db,
            main_page_id,
            root: &self.root,
            lock: self.lock.downgrade(),
        }
        .exec(&key)?
        .is_some();

        // If the deletion is not supported, there can not be a hole in the pages.
        #[cfg(not(feature = "delete"))]
        let replace_found = false;

        let mut cur_page = (
            PageId::Main(main_page_id),
            self.db.main_pages.read_page(main_page_id)?.unwrap(),
        );

        loop {
            let overwrite_page = if replace_found {
                cur_page.1.contains(&key)
            } else {
                cur_page.1.contains(&key)
                    || cur_page.1.kv_pairs.len() < self.db.max_kv_per_page as usize
            };

            if overwrite_page {
                let old = cur_page.1.insert(key, value);
                match cur_page.0 {
                    PageId::Main(b) => self.db.main_pages.write_page(b, cur_page.1)?,
                    PageId::Overflow(id) => self.db.overflow_pages.write_page(id, cur_page.1)?,
                }

                if old.is_none() {
                    self.db.n_items.fetch_add(1, Ordering::SeqCst);
                }
                return Ok(old);
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
        let mut tail_page = cur_page;

        // If not, allocate a new overflow page.
        let new_overflow_id = self.db.next_overflow_id.fetch_add(1, Ordering::SeqCst);
        let mut new_page = Page::new();
        new_page.insert(key, value);
        self.db
            .overflow_pages
            .write_page(new_overflow_id, new_page)?;
        // Since sync is only happened when we allocate a new overflow page and it is rare,
        // the performance impact is small.
        self.db.overflow_pages.flush()?;

        // After writing the new overflow page, update the old tail page.
        tail_page.1.overflow_id = Some(new_overflow_id);
        match tail_page.0 {
            PageId::Main(b) => {
                self.db.main_pages.write_page(b, tail_page.1)?;
            }
            PageId::Overflow(id) => {
                self.db.overflow_pages.write_page(id, tail_page.1)?;
            }
        }

        self.db.n_items.fetch_add(1, Ordering::SeqCst);

        Ok(None)
    }
}
