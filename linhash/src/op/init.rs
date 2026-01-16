use super::*;

pub struct Init<'a> {
    pub db: &'a LinHashCore,
}

impl Init<'_> {
    pub fn exec(self) -> Result<()> {
        // Insert two empty pages if the main pages are not initialized.
        let mut init_page = Page::new();
        init_page.locallevel = Some(1);

        self.db.main_pages.write_page(0, &init_page)?;
        self.db.main_pages.write_page(1, &init_page)?;

        Ok(())
    }
}
