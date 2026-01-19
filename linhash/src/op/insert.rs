use super::*;

pub struct Insert<'a> {
    pub db: &'a LinHashCore,
    pub chain_id: PageChainId,
    #[allow(unused)]
    pub root: RwLockReadGuard<'a, Root>,
    #[allow(unused)]
    pub lock: lock::SelectiveLockGuard<'a>,
}

impl Insert<'_> {
    pub fn exec(self, key: Vec<u8>, value: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let chain_id = self.chain_id;

        let mut pages = VecDeque::new();

        let next_page = (
            PageId::Primary(chain_id.primary_page_id),
            self.db
                .primary_pages
                .read_page(chain_id.primary_page_id)?
                .unwrap(),
        );

        if next_page.1.locallevel != Some(chain_id.locallevel) {
            return Err(Error::LocalLevelMismatch);
        }

        pages.push_back(next_page);

        loop {
            let cur_page = pages.back_mut().unwrap();

            if cur_page.1.contains(&key) {
                let old = cur_page.1.insert(key, value);
                match cur_page.0 {
                    PageId::Primary(b) => self.db.primary_pages.write_page(b, &cur_page.1)?,
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
                    PageId::Primary(b) => self.db.primary_pages.write_page(b, &cur_page.1)?,
                    PageId::Overflow(id) => self.db.overflow_pages.write_page(id, &cur_page.1)?,
                }
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
            PageId::Primary(b) => {
                self.db.primary_pages.write_page(b, &tail_page.1)?;
            }
            PageId::Overflow(id) => {
                self.db.overflow_pages.write_page(id, &tail_page.1)?;
            }
        }

        Ok(None)
    }
}
