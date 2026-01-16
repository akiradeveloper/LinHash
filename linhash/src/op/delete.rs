use super::*;

pub struct Delete<'a> {
    pub db: &'a LinHashCore,
    pub chain_id: PageChainId,
    #[allow(unused)]
    pub root: RwLockReadGuard<'a, Root>,
    #[allow(unused)]
    pub lock: lock::ExclusiveLockGuard<'a>,
}

impl Delete<'_> {
    pub fn exec(self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let chain_id = self.chain_id;

        let mut cur_page = (
            PageId::Main(chain_id.main_page_id),
            self.db
                .main_pages
                .read_page(chain_id.main_page_id)?
                .unwrap(),
        );

        if cur_page.1.locallevel != Some(chain_id.locallevel) {
            return Err(Error::LocalLevelMismatch);
        }

        loop {
            if cur_page.1.contains(key) {
                let removed = cur_page.1.kv_pairs.remove(key);
                match cur_page.0 {
                    PageId::Main(b) => self.db.main_pages.write_page(b, &cur_page.1)?,
                    PageId::Overflow(id) => self.db.overflow_pages.write_page(id, &cur_page.1)?,
                }

                return Ok(removed);
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

        Ok(None)
    }
}
