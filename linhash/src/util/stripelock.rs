use super::*;

pub struct ReadLockGuard<'a>(RwLockReadGuard<'a, ()>);

pub struct SelectiveLockGuard<'a>(RwLockReadGuard<'a, ()>, MutexGuard<'a, ()>);

impl<'a> SelectiveLockGuard<'a> {
    pub fn downgrade(self) -> ReadLockGuard<'a> {
        ReadLockGuard(self.0)
    }
}

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

    pub fn selective_lock(&self, id: u64) -> SelectiveLockGuard<'_> {
        let b = (id as usize) % self.n;
        let g1 = self.rwlocks[b].read();
        let g2 = self.mutexes[b].lock();
        SelectiveLockGuard(g1, g2)
    }
}
