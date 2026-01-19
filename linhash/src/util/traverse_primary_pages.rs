use super::*;

pub struct TraversePrimaryPages<'a> {
    pub db: &'a LinHashCore,
}

impl TraversePrimaryPages<'_> {
    /// Returns the number of valid primary pages
    pub fn exec(self) -> Result<u64> {
        let n_simple = self.traverse_primary_pages_simple()?;
        if n_simple < 2 {
            return Ok(n_simple);
        }

        let last_page_id = n_simple - 1;

        let last_page = self.db.primary_pages.read_page_ref(last_page_id)?.unwrap();
        let locallevel = last_page.locallevel().unwrap();
        let old_page_id = calc_old_page_id(last_page_id, locallevel);
        let old_page = self.db.primary_pages.read_page_ref(old_page_id)?.unwrap();

        // If both pages are committed, their locallevels must be the same.
        let old_locallevel = old_page.locallevel().unwrap();
        if old_locallevel == locallevel {
            Ok(n_simple)
        } else {
            Ok(n_simple - 1)
        }
    }

    pub fn traverse_primary_pages_simple(&self) -> Result<u64> {
        let page0 = self.db.primary_pages.read_page_ref(0)?;
        if page0.is_none() {
            return Ok(0);
        }

        let page1 = self.db.primary_pages.read_page_ref(1)?;
        if page1.is_none() {
            return Ok(1);
        }

        for page_id in 2.. {
            let Some(_) = self.db.primary_pages.read_page_ref(page_id)? else {
                return Ok(page_id);
            };
        }

        unreachable!()
    }
}

// Drop the highest bit to get the old page id.
fn calc_old_page_id(page_id: u64, locallevel: u8) -> u64 {
    let old_page_id_mask = ((1 << locallevel) - 1) >> 1;
    page_id & old_page_id_mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calc_old_page_id() {
        let tbl = [
            (0b10, 2, 0b0),
            (0b11, 2, 0b1),
            (0b100, 3, 0b0),
            (0b101, 3, 0b01),
            (0b110, 3, 0b10),
            (0b111, 3, 0b11),
        ];

        for (page_id, locallevel, expected_old_page_id) in tbl {
            assert_eq!(calc_old_page_id(page_id, locallevel), expected_old_page_id);
        }
    }
}
