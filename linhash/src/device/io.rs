use super::*;

use rustix::fd::OwnedFd;
use rustix::fs::{FallocateFlags, fallocate};
use rustix::fs::{Mode, OFlags, fdatasync, open};
use rustix::io::{ReadWriteFlags, preadv2, pwritev2};

pub struct IO {
    fd: OwnedFd,
}

impl IO {
    pub fn new(p: &Path) -> Result<Self> {
        let fd = open(
            p,
            OFlags::RDWR | OFlags::CREATE | OFlags::DIRECT,
            Mode::from_bits_truncate(0o600),
        )?;

        Ok(Self { fd })
    }

    pub fn read(&self, buf: &mut PageIOBuffer, offset: u64) -> Result<()> {
        let mut io_vec = [rustix::io::IoSliceMut::new(buf.as_mut_slice())];
        preadv2(&self.fd, &mut io_vec, offset, ReadWriteFlags::empty())?;

        Ok(())
    }

    pub fn write(&self, buf: &PageIOBuffer, offset: u64) -> Result<()> {
        let atomic_flag = ReadWriteFlags::from_bits_retain(libc::RWF_ATOMIC as u32);

        let mut flags = ReadWriteFlags::empty();
        flags.insert(atomic_flag);

        let io_vec = [rustix::io::IoSlice::new(buf.as_slice())];
        pwritev2(&self.fd, &io_vec, offset, ReadWriteFlags::empty())?;

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        fdatasync(&self.fd)?;
        Ok(())
    }

    pub fn free(&self, offset: u64, len: u64) -> Result<()> {
        fallocate(
            &self.fd,
            FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE,
            offset,
            len,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_read_write() {
        let f = tempfile::NamedTempFile::new().unwrap();
        let io = IO::new(f.path()).unwrap();

        let mut write_buf = PageIOBuffer::new();
        write_buf.resize(4096, 1);
        io.write(&write_buf, 3 * 4096).unwrap();

        let mut read_buf = PageIOBuffer::with_capacity(4096);
        read_buf.resize(4096, 0);
        assert_ne!(write_buf.as_slice(), read_buf.as_slice());

        io.read(&mut read_buf, 3 * 4096).unwrap();

        assert_eq!(write_buf.as_slice(), read_buf.as_slice());
    }
}
