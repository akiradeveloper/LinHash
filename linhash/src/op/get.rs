use super::*;

pub struct Get<'a> {
    pub db: &'a LinHashCore,
    pub main_page_id: u64,
    #[allow(unused)]
    pub root: &'a RwLockReadGuard<'a, Root>,
    #[allow(unused)]
    pub lock: util::ReadLockGuard<'a>,
}

impl Get<'_> {
    pub fn exec(self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let main_page_id = self.main_page_id;

        let mut page = self.db.main_pages.read_page_ref(main_page_id)?.unwrap();

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
