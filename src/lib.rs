use arc_swap::{ArcSwap, ArcSwapOption};
use crossbeam::queue::ArrayQueue;
use std::{sync::Arc, thread::JoinHandle};

const GARBAGE_QUEUE_SIZE: usize = 256;
const GARBAGE_COLLECTOR_TIMEOUT_MS: u64 = 50;

/// Read-Clone-Update (RCU) wrapper around a shared object. Readers never block writers, copies are
/// dropped on a concurrent garbage collector thread on a best effort case.
///
/// Best effort is when fewer than `GARBAGE_QUEUE_SIZE` many writes occur before they can be
/// collected by the GC thread, which runs on an interval of at most `GARBAGE_COLLECTOR_TIMEOUT_MS`,
/// or if the reader is the last owner of the shared data.
#[derive(Clone)]
pub struct Rcu<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    value: ArcSwap<T>,
    garbage: ArrayQueue<Arc<T>>,
    collector: ArcSwapOption<JoinHandle<()>>,
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // When `Drop` is called the refcount of the outer Arc will have dropped to zero, so the
        // Weak<Inner> held by the collector thread will return `None` when upgraded, allowing the
        // thread to be joined
        if let Some(collector) = self.collector.swap(None).take() {
            collector.thread().unpark();
        }
    }
}

unsafe impl<T> Sync for Inner<T> {}
unsafe impl<T: Send> Send for Inner<T> {}

impl<T> Rcu<T>
where
    T: Send + 'static,
{
    /// Create a new Rcu wrapped value.
    pub fn new(value: T) -> Self {
        let value = ArcSwap::from_pointee(value);
        let garbage = ArrayQueue::new(GARBAGE_QUEUE_SIZE);
        let collector = ArcSwapOption::empty();
        let inner = Arc::new(Inner {
            value,
            garbage,
            collector,
        });
        let collector = std::thread::spawn({
            let inner = Arc::downgrade(&inner);
            move || loop {
                std::thread::park_timeout(std::time::Duration::from_millis(
                    GARBAGE_COLLECTOR_TIMEOUT_MS,
                ));
                let Some(inner) = inner.upgrade() else {
                    break;
                };
                while let Some(_value) = inner.garbage.pop() {}
            }
        });
        inner.collector.store(Some(Arc::new(collector)));
        Self { inner }
    }

    /// Read, clone, and update the internal value.
    pub fn rcu(&self, f: impl FnMut(&Arc<T>) -> T) {
        let old = self.inner.value.rcu(f);
        self.inner.garbage.push(old).ok();
    }

    /// Load the current value.
    pub fn read(&self) -> Arc<T> {
        self.inner.value.load_full()
    }
}
