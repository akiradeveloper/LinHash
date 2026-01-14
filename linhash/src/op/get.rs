use super::*;

pub struct Get<'a> {
    pub db: &'a LinHash,
    pub lock: PageLock,
}

impl Get<'_> {
    pub fn exec(self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut page = self.db.main_pages.read_page_ref(self.lock.0)?.unwrap();

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
