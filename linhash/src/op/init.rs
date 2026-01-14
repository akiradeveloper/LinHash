use super::*;

pub struct Init<'a> {
    pub db: &'a mut LinHash,
}

impl Init<'_> {
    pub fn exec(self) -> Result<()> {
        // Insert two empty pages if the main pages are not initialized.
        self.db.main_pages.write_page(0, Page::new())?;
        self.db.main_pages.write_page(1, Page::new())?;

        Ok(())
    }
}
