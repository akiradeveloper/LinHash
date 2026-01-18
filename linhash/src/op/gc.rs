use super::*;

pub struct GC<'a> {
    pub db: &'a LinHashCore,
    pub root: Root,
}

impl GC<'_> {
    pub fn exec(self) -> Result<()> {
        let range = util::TraverseOverflow {
            db: self.db,
            root: self.root,
        }
        .exec()?;

        self.db.overflow_pages.free_page_range(0, range.start)?;

        Ok(())
    }
}
