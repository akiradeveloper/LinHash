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

        let mut page = self
            .db
            .main_pages
            .read_page_ref(chain_id.main_page_id)?
            .unwrap();

        if page.locallevel() != Some(chain_id.locallevel) {
            return Err(Error::LocalLevelMismatch);
        }

        loop {
            if let Some(v) = page.get_value(key) {
                return Ok(Some(v.to_owned()));
            }

            match page.overflow_id() {
                Some(id) => {
                    page = self.db.overflow_pages.read_page_ref(id)?.unwrap();
                }
                None => {
                    return Ok(None);
                }
            }
        }
    }
}
