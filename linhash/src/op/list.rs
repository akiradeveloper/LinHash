use super::*;

use genawaiter::sync::{Co, Gen};

pub struct List<'a> {
    pub db: &'a LinHashCore,
    #[allow(unused)]
    pub root: RwLockReadGuard<'a, Root>,
}

impl List<'_> {
    pub fn exec(self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        Gen::new(move |co: Co<(Vec<u8>, Vec<u8>)>| async move {
            let mut page_id = 0;

            loop {
                let Ok(Some(_)) = self.db.main_pages.read_page(page_id) else {
                    return;
                };

                let it = ListOnce {
                    db: self.db,
                    main_page_id: page_id,
                    root: &self.root,
                    lock: self.db.locks.read_lock(page_id),
                }
                .exec();

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
    pub main_page_id: u64,
    #[allow(unused)]
    pub root: &'a RwLockReadGuard<'a, Root>,
    #[allow(unused)]
    pub lock: lock::ReadLockGuard<'a>,
}

impl ListOnce<'_> {
    pub fn exec(self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> {
        Gen::new(move |co: Co<(Vec<u8>, Vec<u8>)>| async move {
            let Ok(Some(next_page)) = self.db.main_pages.read_page(self.main_page_id) else {
                return;
            };

            let mut page = next_page;
            loop {
                for (k, v) in page.kv_pairs {
                    co.yield_((k, v)).await
                }

                match page.overflow_id {
                    Some(id) => {
                        let Ok(Some(next_page)) = self.db.overflow_pages.read_page(id) else {
                            return;
                        };
                        page = next_page;
                    }
                    None => return,
                }
            }
        })
        .into_iter()
    }
}
