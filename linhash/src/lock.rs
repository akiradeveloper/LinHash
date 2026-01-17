use super::*;

pub struct ReadLockGuard<'a>(#[allow(unused)] RwLockReadGuard<'a, ()>);

pub struct SelectiveLockGuard<'a>(
    #[allow(unused)] RwLockReadGuard<'a, ()>,
    #[allow(unused)] MutexGuard<'a, ()>,
);

pub struct ExclusiveLockGuard<'a>(#[allow(unused)] RwLockWriteGuard<'a, ()>);

pub struct StripeLock {
    n: usize,
    rwlocks: Vec<RwLock<()>>,
    mutexes: Vec<Mutex<()>>,
}

impl StripeLock {
    pub fn new(n: usize) -> Self {
        let mut rwlocks = Vec::with_capacity(n);
        for _ in 0..n {
            rwlocks.push(RwLock::new(()));
        }
        let mut mutexes = Vec::with_capacity(n);
        for _ in 0..n {
            mutexes.push(Mutex::new(()));
        }
        Self {
            n,
            rwlocks,
            mutexes,
        }
    }

    pub fn read_lock(&self, id: u64) -> ReadLockGuard<'_> {
        let b = (id as usize) % self.n;
        ReadLockGuard(self.rwlocks[b].read())
    }

    pub fn try_read_lock(&self, id: u64) -> Option<ReadLockGuard<'_>> {
        let b = (id as usize) % self.n;
        match self.rwlocks[b].try_read() {
            Some(g) => Some(ReadLockGuard(g)),
            None => None,
        }
    }

    pub fn selective_lock(&self, id: u64) -> SelectiveLockGuard<'_> {
        let b = (id as usize) % self.n;
        let g1 = self.rwlocks[b].read();
        let g2 = self.mutexes[b].lock();
        SelectiveLockGuard(g1, g2)
    }

    pub fn try_selective_lock(&self, id: u64) -> Option<SelectiveLockGuard<'_>> {
        let b = (id as usize) % self.n;
        let g1 = match self.rwlocks[b].try_read() {
            Some(g) => g,
            None => return None,
        };
        let g2 = match self.mutexes[b].try_lock() {
            Some(g) => g,
            None => return None,
        };
        Some(SelectiveLockGuard(g1, g2))
    }

    pub fn exclusive_lock(&self, id: u64) -> ExclusiveLockGuard<'_> {
        let b = (id as usize) % self.n;
        ExclusiveLockGuard(self.rwlocks[b].write())
    }

    pub fn try_exclusive_lock(&self, id: u64) -> Option<ExclusiveLockGuard<'_>> {
        let b = (id as usize) % self.n;
        match self.rwlocks[b].try_write() {
            Some(g) => Some(ExclusiveLockGuard(g)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_read_read_ok() {
        let lock = StripeLock::new(4);
        let g1 = lock.read_lock(0);
        let g2 = lock.read_lock(0);
    }

    #[test]
    fn test_lock_selective_read_ok() {
        let lock = StripeLock::new(4);
        let g1 = lock.selective_lock(0);
        let g2 = lock.read_lock(0);
    }

    #[test]
    fn test_lock_selective_selective_fail() {
        let lock = StripeLock::new(4);
        let g1 = lock.selective_lock(0);
        let g2 = lock.try_selective_lock(0);
        assert_eq!(g2.is_none(), true);
    }

    #[test]
    fn test_selective_exclusive_fail() {
        let lock = StripeLock::new(4);
        let g1 = lock.selective_lock(0);
        let g2 = lock.try_exclusive_lock(0);
        assert_eq!(g2.is_none(), true);
    }

    #[test]
    fn test_exclusive_exclusive_fail() {
        let lock = StripeLock::new(4);
        let g1 = lock.exclusive_lock(0);
        let g2 = lock.try_exclusive_lock(0);
        assert_eq!(g2.is_none(), true);
    }
}
