use super::*;

use rustix::fd::OwnedFd;
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

    pub fn write(&self, buf: &[u8], offset: u64) -> Result<()> {
        let n = buf.len();
        dbg!(&n);

        let mut io_buf = PageIOBuffer::with_capacity(n);
        io_buf.extend_from_slice(&buf);
        // The buffer size should be multiple of 4096 bytes.
        io_buf.resize(4096, 0);

        let io_vec = [rustix::io::IoSlice::new(io_buf.as_mut_slice())];
        pwritev2(&self.fd, &io_vec, offset, ReadWriteFlags::empty())?;

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        fdatasync(&self.fd)?;
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

        let write_buf = vec![1u8; 4096];
        io.write(&write_buf, 3 * 4096).unwrap();

        let mut read_buf = PageIOBuffer::with_capacity(4096);
        read_buf.resize(4096, 0);
        assert_ne!(write_buf, read_buf.as_slice());

        io.read(&mut read_buf, 3 * 4096).unwrap();

        assert_eq!(write_buf, read_buf.as_slice());
    }
}
