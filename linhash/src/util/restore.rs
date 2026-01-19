use super::*;

pub struct Restore<'a> {
    pub db: &'a LinHashCore,
}

impl Restore<'_> {
    /// Returns the number of valid primary pages.
    pub fn exec(self) -> Result<u64> {
        let n_primary_pages = util::TraversePrimaryPages { db: self.db }.exec()?;
        if n_primary_pages < 2 {
            return Ok(0);
        }

        let root = restore_root(n_primary_pages);

        let next_overflow_id = {
            let travere_range = util::TraverseOverflowPages { db: self.db, root }.exec()?;
            travere_range.end
        };

        let n_items = self.traverse_all_pages(n_primary_pages)?;

        *self.db.root.write() = root;
        self.db
            .next_overflow_id
            .store(next_overflow_id, Ordering::SeqCst);
        self.db.n_items.store(n_items, Ordering::SeqCst);

        Ok(n_primary_pages)
    }

    /// Returns `n_items`
    fn traverse_all_pages(&self, n_primary_pages: u64) -> Result<u64> {
        let mut n_items = 0;

        for i in 0..n_primary_pages {
            let mut cur_page = self.db.primary_pages.read_page(i)?.unwrap();

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

pub fn restore_root(n_primary_pages: u64) -> Root {
    let bit_width = 64 - n_primary_pages.leading_zeros();
    let msb = 1 << (bit_width - 1);
    let next_split_id = n_primary_pages - msb;

    Root {
        next_split_primary_page_id: next_split_id,
        base_level: bit_width as u8 - 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(next_split_primary_page_id: u64, base_level: u8) -> Root {
        Root {
            next_split_primary_page_id,
            base_level,
        }
    }

    #[test]
    fn test_calc_base_level() {
        assert_eq!(restore_root(2), root(0, 1));
        assert_eq!(restore_root(3), root(1, 1));
        assert_eq!(restore_root(4), root(0, 2));
        assert_eq!(restore_root(5), root(1, 2));
        assert_eq!(restore_root(6), root(2, 2));
        assert_eq!(restore_root(7), root(3, 2));
        assert_eq!(restore_root(8), root(0, 3));
    }
}
