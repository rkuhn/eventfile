use std::sync::{Arc, Mutex};

pub trait Cache {
    fn put(&mut self, key: (u32, u64), value: Arc<[u8]>, prio: bool);
    fn get(&mut self, key: (u32, u64)) -> Option<Arc<[u8]>>;
}

impl<T: Cache> Cache for Arc<Mutex<T>> {
    fn put(&mut self, key: (u32, u64), value: Arc<[u8]>, prio: bool) {
        self.lock().unwrap().put(key, value, prio);
    }

    fn get(&mut self, key: (u32, u64)) -> Option<Arc<[u8]>> {
        self.lock().unwrap().get(key)
    }
}

#[cfg(feature = "pl")]
mod pl {
    use super::Cache;
    use parking_lot::Mutex;
    use std::sync::Arc;

    impl<T: Cache> Cache for Arc<Mutex<T>> {
        fn put(&mut self, key: (u32, u64), value: Arc<[u8]>, prio: bool) {
            self.lock().put(key, value, prio);
        }

        fn get(&mut self, key: (u32, u64)) -> Option<Arc<[u8]>> {
            self.lock().get(key)
        }
    }
}

#[cfg(feature = "fbr")]
mod fbr {
    use super::Cache;
    use fbr_cache::FbrCache;
    use std::sync::Arc;

    impl<const C: usize> Cache for FbrCache<(u32, u64), Arc<[u8]>, C> {
        fn put(&mut self, key: (u32, u64), value: Arc<[u8]>, prio: bool) {
            if prio {
                self.put_prio(key, value);
            } else {
                self.put(key, value);
            }
        }

        fn get(&mut self, key: (u32, u64)) -> Option<Arc<[u8]>> {
            self.get(&key).cloned()
        }
    }
}
