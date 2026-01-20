use super::*;

use linux_raw_sys::general::{STATX_ATTR_WRITE_ATOMIC, STATX_WRITE_ATOMIC};
use rustix::fs::{self, AtFlags, StatxFlags};

pub struct AtomicSupport {
    pub dio_offset_align: u32,
    pub dio_mem_align: u32,
    pub dio_read_mem_align: u32,
    pub atomic_write_unit_min: u32,
    pub atomic_write_unit_max: u32,
    pub atomic_write_segments_max: u32,
    pub atomic_write_unit_max_opt: u32,
}

pub fn print_atomic_support(path: &Path) -> Result<Option<AtomicSupport>> {
    let want = StatxFlags::DIOALIGN | StatxFlags::from_bits_retain(STATX_WRITE_ATOMIC);

    let st = fs::statx(fs::CWD, path, AtFlags::SYMLINK_NOFOLLOW, want)?;
    dbg!(&st);

    let has_valid_field = st.stx_mask & STATX_WRITE_ATOMIC != 0;

    let supported = (st.stx_attributes_mask.bits() & STATX_ATTR_WRITE_ATOMIC as u64 != 0)
        && (st.stx_attributes.bits() & STATX_ATTR_WRITE_ATOMIC as u64 != 0);

    if !(supported && has_valid_field) {
        return Ok(None);
    }

    Ok(Some(AtomicSupport {
        dio_offset_align: st.stx_dio_offset_align,
        dio_read_mem_align: st.stx_dio_mem_align,
        dio_mem_align: st.stx_dio_mem_align,
        atomic_write_unit_min: st.stx_atomic_write_unit_min,
        atomic_write_unit_max: st.stx_atomic_write_unit_max,
        atomic_write_segments_max: st.stx_atomic_write_segments_max,
        atomic_write_unit_max_opt: st.stx_atomic_write_unit_max_opt,
    }))
}
