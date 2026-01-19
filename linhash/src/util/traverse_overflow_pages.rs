use super::*;

/// [start, end)
pub struct OverflowRange {
    pub start: u64,
    pub end: u64,
}

/// Traverse all primary pages and find the range of overflow pages used.
pub struct TraverseOverflowPages<'a> {
    pub db: &'a LinHashCore,
    pub root: Root,
}

impl TraverseOverflowPages<'_> {
    pub fn exec(self) -> Result<OverflowRange> {
        let mut min = u64::MAX;
        let mut max = 0;

        for page_id in 0..self.root.calc_n_pages() {
            let page = self.db.primary_pages.read_page_ref(page_id)?.unwrap();

            let mut cur_page = page;
            loop {
                match cur_page.overflow_id() {
                    Some(id) => {
                        cur_page = self.db.overflow_pages.read_page_ref(id)?.unwrap();
                        min = min.min(id);
                        max = max.max(id + 1);
                    }
                    None => {
                        break;
                    }
                }
            }
        }

        if min == u64::MAX {
            return Ok(OverflowRange { start: 0, end: 0 });
        }

        Ok(OverflowRange {
            start: min,
            end: max,
        })
    }
}
