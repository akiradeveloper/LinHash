use super::*;

pub struct Get<'a> {
    pub db: &'a LinHashCore,
    pub chain_id: PageChainId,
    #[allow(unused)]
    pub root: RwLockReadGuard<'a, Root>,
    #[allow(unused)]
    pub lock: lock::ReadLockGuard<'a>,
}

impl Get<'_> {
    pub fn exec(self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let chain_id = self.chain_id;

        let mut hops = 0;

        let mut page = self
            .db
            .main_pages
            .read_page_ref(chain_id.main_page_id)?
            .unwrap();
        hops += 1;

        if page.locallevel() != Some(chain_id.locallevel) {
            return Err(Error::LocalLevelMismatch);
        }

        loop {
            if let Some(v) = page.get_value(key) {
                self.db.stat.lock().push(OpEvent::GetHit(hops));
                return Ok(Some(v.to_owned()));
            }

            match page.overflow_id() {
                Some(id) => {
                    page = self.db.overflow_pages.read_page_ref(id)?.unwrap();
                    hops += 1;
                }
                None => {
                    self.db.stat.lock().push(OpEvent::GetMiss(hops));
                    return Ok(None);
                }
            }
        }
    }
}
