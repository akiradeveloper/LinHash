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

        let mut pages = VecDeque::new();

        let next_page = (
            PageId::Main(main_page_id),
            self.db.main_pages.read_page(main_page_id)?.unwrap(),
        );
        pages.push_back(next_page);

        loop {
            let cur_page = pages.back_mut().unwrap();

            if cur_page.1.contains(&key) {
                let old = cur_page.1.insert(key, value);
                match cur_page.0 {
                    PageId::Main(b) => self.db.main_pages.write_page(b, &cur_page.1)?,
                    PageId::Overflow(id) => self.db.overflow_pages.write_page(id, &cur_page.1)?,
                }
                return Ok(old);
            }

            if let Some(overflow_id) = cur_page.1.overflow_id {
                let next_page = (
                    PageId::Overflow(overflow_id),
                    self.db.overflow_pages.read_page(overflow_id)?.unwrap(),
                );
                pages.push_back(next_page);
            } else {
                break;
            }
        }

        for cur_page in &mut pages {
            if cur_page.1.kv_pairs.len() < self.db.max_kv_per_page as usize {
                cur_page.1.insert(key, value);
                match cur_page.0 {
                    PageId::Main(b) => self.db.main_pages.write_page(b, &cur_page.1)?,
                    PageId::Overflow(id) => self.db.overflow_pages.write_page(id, &cur_page.1)?,
                }
                self.db.n_items.fetch_add(1, Ordering::SeqCst);
                return Ok(None);
            }
        }

        let tail_page = pages.back_mut().unwrap();

        // If not, allocate a new overflow page.
        let new_overflow_id = self.db.next_overflow_id.fetch_add(1, Ordering::SeqCst);
        let mut new_page = Page::new();
        new_page.insert(key, value);
        self.db
            .overflow_pages
            .write_page(new_overflow_id, &new_page)?;
        // Since sync is only happened when we allocate a new overflow page and it is rare,
        // the performance impact is small.
        self.db.overflow_pages.flush()?;

        // After writing the new overflow page, update the old tail page.
        tail_page.1.overflow_id = Some(new_overflow_id);
        match tail_page.0 {
            PageId::Main(b) => {
                self.db.main_pages.write_page(b, &tail_page.1)?;
            }
            PageId::Overflow(id) => {
                self.db.overflow_pages.write_page(id, &tail_page.1)?;
            }
        }

        self.db.n_items.fetch_add(1, Ordering::SeqCst);

        Ok(None)
    }
}
