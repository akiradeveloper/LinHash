use super::*;

pub struct Restore<'a> {
    pub db: &'a LinHashCore,
}

impl Restore<'_> {
    /// Retruns the number of valid main pages.
    pub fn exec(self) -> Result<u64> {
        let n_main_pages = self.traverse_main_pages()?;
        if n_main_pages < 2 {
            return Ok(0);
        }

        let (next_split_main_page_id, main_base_level) = calc_base_level(n_main_pages);

        let next_overflow_id = {
            let root = self.db.root.read();
            let travere_range = op::TraverseOverflow { db: self.db, root }.exec()?;

            travere_range.end
        };

        let n_items = self.traverse_all_pages(n_main_pages)?;

        self.db.root.write().base_level = main_base_level;
        self.db.root.write().next_split_main_page_id = next_split_main_page_id;
        self.db
            .next_overflow_id
            .store(next_overflow_id, Ordering::SeqCst);
        self.db.n_items.store(n_items, Ordering::SeqCst);

        Ok(n_main_pages)
    }

    /// Returns the number of valid main pages
    fn traverse_main_pages(&self) -> Result<u64> {
        for i in 0.. {
            let Some(_) = self.db.main_pages.read_page(i)? else {
                return Ok(i);
            };
        }

        unreachable!()
    }

    /// Returns `next_overflow_id`
    fn traverse_overflow_pages(&self) -> Result<u64> {
        for i in 0.. {
            let Some(_) = self.db.overflow_pages.read_page(i)? else {
                return Ok(i);
            };
        }

        unreachable!()
    }

    /// Returns `n_items`
    fn traverse_all_pages(&self, main_page_until: u64) -> Result<u64> {
        let mut n_items = 0;

        for i in 0..main_page_until {
            let mut cur_page = self.db.main_pages.read_page(i)?.unwrap();

            loop {
                n_items += cur_page.kv_pairs.len() as u64;

                if let Some(overflow_id) = cur_page.overflow_id {
                    cur_page = self.db.overflow_pages.read_page(overflow_id)?.unwrap();
                } else {
                    break;
                }
            }
        }

        Ok(n_items)
    }
}

/// Returns `next_split_main_page_id` and `main_base_level`
pub fn calc_base_level(n_main_pages: u64) -> (u64, u8) {
    let bit_width = 64 - n_main_pages.leading_zeros();
    let msb = 1 << (bit_width - 1);
    let next_split_id = n_main_pages - msb;
    (next_split_id, bit_width as u8 - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calc_base_level() {
        assert_eq!(calc_base_level(2), (0, 1));
        assert_eq!(calc_base_level(3), (1, 1));
        assert_eq!(calc_base_level(4), (0, 2));
        assert_eq!(calc_base_level(5), (1, 2));
        assert_eq!(calc_base_level(6), (2, 2));
        assert_eq!(calc_base_level(7), (3, 2));
        assert_eq!(calc_base_level(8), (0, 3));
    }
}
