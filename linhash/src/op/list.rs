use super::*;

use genawaiter::sync::{Co, Gen};

pub struct List<'a> {
    pub db: &'a LinHashCore,
    #[allow(unused)]
    pub root: RwLockWriteGuard<'a, Root>,
}

impl List<'_> {
    pub fn exec(self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        Gen::new(move |co: Co<(Vec<u8>, Vec<u8>)>| async move {
            let mut page_id = 0;

            loop {
                // Stop if valid main page does not exist.
                let Ok(Some(page)) = self.db.main_pages.read_page_ref(page_id) else {
                    return;
                };

                let it = ListOnce { db: self.db, page }.exec();

                for (k, v) in it {
                    co.yield_((k, v)).await;
                }

                page_id += 1;
            }
        })
        .into_iter()
    }
}

struct ListOnce<'a> {
    pub db: &'a LinHashCore,
    pub page: PageRef,
}

impl ListOnce<'_> {
    pub fn exec(self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        Gen::new(move |co: Co<(Vec<u8>, Vec<u8>)>| async move {
            let mut cur_page = self.page;

            loop {
                for (k, v) in cur_page.kv_pairs() {
                    co.yield_((k.to_vec(), v.to_vec())).await
                }

                match cur_page.overflow_id() {
                    Some(id) => {
                        let Ok(Some(next_page)) = self.db.overflow_pages.read_page_ref(id) else {
                            return;
                        };
                        cur_page = next_page;
                    }
                    None => return,
                }
            }
        })
        .into_iter()
    }
}
