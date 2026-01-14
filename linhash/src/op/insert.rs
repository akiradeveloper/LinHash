use super::*;

pub struct Insert<'a> {
    pub db: &'a mut LinHash,
}

impl Insert<'_> {
    pub fn exec(
        self,
        key: Vec<u8>,
        value: Vec<u8>,
        existence_confirmed: bool,
    ) -> Result<Option<Vec<u8>>> {
        // The `max_kv_per_page` is a fixed value so the size of key and value must be fixed.
        if self.db.max_kv_per_page.is_none() {
            self.db.max_kv_per_page = Some(calc_max_kv_per_page(key.len(), value.len()));
        }

        let b = self.db.calc_main_page_id(&key);

        let mut cur_page = (PageId::Main(b), self.db.main_pages.read_page(b)?.unwrap());

        loop {
            let overwrite_page = if existence_confirmed {
                cur_page.1.contains(&key)
            } else {
                cur_page.1.contains(&key)
                    || cur_page.1.kv_pairs.len() < self.db.max_kv_per_page.unwrap() as usize
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
