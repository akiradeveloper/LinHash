use super::*;

pub struct Split<'a> {
    pub db: &'a mut ForeverHash,
}

impl Split<'_> {
    /// Split the main page at `next_split_id` into two main pages.
    pub fn exec(mut self) -> Result<()> {
        let kv_pairs = self.collect_rehash_kv_pairs()?;

        let page_chains = self.insert_kv_pairs_into_pages(kv_pairs);

        // Write from bigger main page id (new one) to avoid losing pairs on crash.
        for (_, page_chain) in page_chains.into_iter().rev() {
            // Write from overflow pages.
            for (page_id, page) in page_chain.into_iter().rev() {
                match page_id {
                    PageId::Main(id) => {
                        // Before commiting the main page, ensure that overflow pages is persisted.
                        // Since split is rare, performance impact by sync call is small.
                        self.db.overflow_pages.flush()?;
                        self.db.main_pages.write_page_atomic(id, page)?;
                        // We don't need to sync the main page because losing the main page doesn't affect consistency.
                    }
                    PageId::Overflow(id) => {
                        self.db.overflow_pages.write_page(id, page)?;
                    }
                }
            }
        }

        self.inc_split_id();

        Ok(())
    }

    // Collect all the kv-pairs which is reachable from the main page at `next_split_id`.
    fn collect_rehash_kv_pairs(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let split_id = self.db.next_split_main_page_id;

        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        let mut cur_page = self.db.main_pages.read_page(split_id)?.unwrap();
        loop {
            for (k, v) in cur_page.kv_pairs.drain() {
                out.push((k, v));
            }

            match cur_page.overflow_id {
                Some(id) => {
                    cur_page = self.db.overflow_pages.read_page(id)?.unwrap();
                }
                None => {
                    break;
                }
            }
        }

        Ok(out)
    }

    fn insert_kv_pairs_into_pages(
        &mut self,
        kv_pairs: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> BTreeMap<u64, VecDeque<(PageId, Page)>> {
        let split_id = self.db.next_split_main_page_id;
        let cur_level = self.db.main_base_level;

        let mut page_chains = BTreeMap::new();
        let new_split_id = split_id + (1 << cur_level);
        page_chains.insert(split_id, VecDeque::new());
        page_chains.insert(new_split_id, VecDeque::new());
        for (&main_page_id, page_chain) in &mut page_chains {
            page_chain.push_back((PageId::Main(main_page_id), Page::new()));
        }

        for (k, v) in kv_pairs {
            let hash = self.db.hash_key(&k);
            let b = hash & ((1 << (cur_level + 1)) - 1);
            let tail = page_chains.get_mut(&b).unwrap().back_mut().unwrap();

            if tail.1.kv_pairs.len() < self.db.max_kv_per_page.unwrap() as usize {
                tail.1.insert(k, v);
                continue;
            } else {
                let new_overflow_id = self.db.next_overflow_id;
                self.db.next_overflow_id += 1;
                tail.1.overflow_id = Some(new_overflow_id);

                let mut new_page = Page::new();
                new_page.insert(k, v);

                page_chains
                    .get_mut(&b)
                    .unwrap()
                    .push_back((PageId::Overflow(new_overflow_id), new_page));
            }
        }

        page_chains
    }

    // Only this function updates `next_split_main_page_id` and `main_base_level`.
    fn inc_split_id(&mut self) {
        self.db.next_split_main_page_id += 1;
        if self.db.next_split_main_page_id == (1 << self.db.main_base_level) {
            self.db.main_base_level += 1;
            self.db.next_split_main_page_id = 0;
        }
    }
}
