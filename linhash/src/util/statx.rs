use super::*;

use rustix::fs::{self, AtFlags, StatxFlags};
use linux_raw_sys::general::{STATX_ATTR_WRITE_ATOMIC, STATX_WRITE_ATOMIC};

pub fn print_atomic_support(path: &Path) -> Result<()> {
    let want = StatxFlags::DIOALIGN |
        StatxFlags::from_bits_retain(STATX_WRITE_ATOMIC);

    let st = fs::statx(
        fs::CWD, 
        path,
        AtFlags::SYMLINK_NOFOLLOW,
        want)?;

    let has_atomic_field  = st.stx_mask & STATX_WRITE_ATOMIC != 0;

    dbg!(&st, has_atomic_field);

    let supports = st.stx_attributes_mask.bits() & STATX_ATTR_WRITE_ATOMIC;
    let supports2 = st.stx_attributes.bits() & STATX_WRITE_ATOMIC;

    Ok(())
}