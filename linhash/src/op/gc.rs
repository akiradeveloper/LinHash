use super::*;

pub struct GC<'a> {
    pub db: &'a LinHashCore,
    pub root: RwLockReadGuard<'a, Root>,
}

impl GC<'_> {
    pub fn exec(self) -> Result<u64> {
        let mut n_deleted = 0;

        let range = op::TraverseOverflow {
            db: self.db,
            root: self.root,
        }
        .exec()?;

        for id in 0..range.start {
            self.db.overflow_pages.free_page(id)?;
            n_deleted += 1;
        }

        Ok(n_deleted)
    }
}
