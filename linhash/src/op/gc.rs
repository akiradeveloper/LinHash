use super::*;

use std::collections::HashSet;

pub struct GC<'a> {
    pub db: &'a LinHashCore,
    pub root: RwLockReadGuard<'a, Root>,
}  

impl GC<'_> {
    pub fn exec(self) -> Result<u64> {
        let next_overflow_id = self.db.next_overflow_id.load(Ordering::SeqCst);
        let mut delete_overflow_ids = (0..next_overflow_id).collect::<HashSet<u64>>();

        for page_id in 0..self.root.calc_n_pages() {
            let page = self.db.main_pages.read_page_ref(page_id)?.unwrap();

            let mut cur_page = page;
            loop {
                match cur_page.overflow_id() {
                    Some(id) => {
                        cur_page = self.db.overflow_pages.read_page_ref(id)?.unwrap();
                        if id < next_overflow_id {
                            delete_overflow_ids.remove(&id);
                        }
                    }
                    None => {
                        break;
                    }
                }
            }
        }

        let mut n_deleted = 0;

        for id in delete_overflow_ids {
            self.db.overflow_pages.free_page(id)?;
            n_deleted += 1;
        }

        Ok(n_deleted)
    }
}