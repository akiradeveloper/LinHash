use super::*;

pub struct Insert<'a> {
    pub db: &'a mut LinHash,
    pub lock: SelectiveLock,
}

impl Insert<'_> {
    pub fn exec(self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        // If the existence of the key is confirmed, insertion will be updating.
        #[cfg(feature = "delete")]
        let replace_found = op::Get {
            db: self.db,
            lock: ReadLock(self.lock.0),
        }
        .exec(&key)?
        .is_some();

        // If the deletion is not supported, there can not be a hole in the pages.
        #[cfg(not(feature = "delete"))]
        let replace_found = false;

        let mut cur_page = (
            PageId::Main(self.lock.0),
            self.db.main_pages.read_page(self.lock.0)?.unwrap(),
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
                    self.db.n_items += 1;
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
        let new_overflow_id = self.db.next_overflow_id;
        self.db.next_overflow_id += 1;
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

        self.db.n_items += 1;

        Ok(None)
    }
}
