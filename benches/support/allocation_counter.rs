use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use fast_dump::sender::file_bench::AllocationCounts;

pub(crate) struct CountingAllocator<A = System> {
    inner: A,
    enabled: AtomicBool,
    allocs: AtomicU64,
    alloc_bytes: AtomicU64,
    reallocs: AtomicU64,
    deallocs: AtomicU64,
    alloc_successes: AtomicU64,
    alloc_success_bytes: AtomicU64,
    realloc_successes: AtomicU64,
    realloc_success_bytes: AtomicU64,
    alloc_failures: AtomicU64,
    realloc_failures: AtomicU64,
    dealloc_bytes: AtomicU64,
    zeroed_bytes: AtomicU64,
}

impl<A> CountingAllocator<A> {
    pub(crate) const fn new(inner: A) -> Self {
        Self {
            inner,
            enabled: AtomicBool::new(false),
            allocs: AtomicU64::new(0),
            alloc_bytes: AtomicU64::new(0),
            reallocs: AtomicU64::new(0),
            deallocs: AtomicU64::new(0),
            alloc_successes: AtomicU64::new(0),
            alloc_success_bytes: AtomicU64::new(0),
            realloc_successes: AtomicU64::new(0),
            realloc_success_bytes: AtomicU64::new(0),
            alloc_failures: AtomicU64::new(0),
            realloc_failures: AtomicU64::new(0),
            dealloc_bytes: AtomicU64::new(0),
            zeroed_bytes: AtomicU64::new(0),
        }
    }

    pub(crate) fn start(&self) {
        // Владелец измерения сериализует интервалы и join всех workers до stop.
        // Reset относится только к операциям: прежние живые allocations остаются живыми.
        assert!(
            !self.enabled.load(Ordering::Relaxed),
            "allocation measurement already active"
        );
        for counter in [
            &self.allocs,
            &self.alloc_bytes,
            &self.reallocs,
            &self.deallocs,
            &self.alloc_successes,
            &self.alloc_success_bytes,
            &self.realloc_successes,
            &self.realloc_success_bytes,
            &self.alloc_failures,
            &self.realloc_failures,
            &self.dealloc_bytes,
            &self.zeroed_bytes,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
        self.enabled.store(true, Ordering::Relaxed);
    }

    pub(crate) fn stop(&self) -> AllocationCounts {
        self.enabled.store(false, Ordering::Relaxed);
        AllocationCounts {
            allocs: self.allocs.load(Ordering::Relaxed),
            alloc_bytes: self.alloc_bytes.load(Ordering::Relaxed),
            reallocs: self.reallocs.load(Ordering::Relaxed),
            deallocs: self.deallocs.load(Ordering::Relaxed),
            alloc_successes: self.alloc_successes.load(Ordering::Relaxed),
            alloc_success_bytes: self.alloc_success_bytes.load(Ordering::Relaxed),
            realloc_successes: self.realloc_successes.load(Ordering::Relaxed),
            realloc_success_bytes: self.realloc_success_bytes.load(Ordering::Relaxed),
            alloc_failures: self.alloc_failures.load(Ordering::Relaxed),
            realloc_failures: self.realloc_failures.load(Ordering::Relaxed),
            dealloc_bytes: self.dealloc_bytes.load(Ordering::Relaxed),
            zeroed_bytes: self.zeroed_bytes.load(Ordering::Relaxed),
        }
    }

    fn request(&self, size: usize) -> bool {
        let enabled = self.enabled.load(Ordering::Relaxed);
        if enabled {
            // Старые поля сохраняют все requests, включая realloc и неудачи.
            self.allocs.fetch_add(1, Ordering::Relaxed);
            self.alloc_bytes.fetch_add(size as u64, Ordering::Relaxed);
        }
        enabled
    }

    fn allocated(&self, pointer: *mut u8, size: usize, zeroed: bool) {
        if pointer.is_null() {
            self.alloc_failures.fetch_add(1, Ordering::Relaxed);
        } else {
            self.alloc_successes.fetch_add(1, Ordering::Relaxed);
            self.alloc_success_bytes
                .fetch_add(size as u64, Ordering::Relaxed);
            if zeroed {
                self.zeroed_bytes.fetch_add(size as u64, Ordering::Relaxed);
            }
        }
    }
}

// Обёртка не выделяет память. Тот же тип используется глобально только отдельным
// diagnostic binary; тесты создают локальные instances с независимыми счётчиками.
unsafe impl<A: GlobalAlloc> GlobalAlloc for CountingAllocator<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let enabled = self.request(layout.size());
        let pointer = unsafe { self.inner.alloc(layout) };
        if enabled {
            self.allocated(pointer, layout.size(), false);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let enabled = self.request(layout.size());
        let pointer = unsafe { self.inner.alloc_zeroed(layout) };
        if enabled {
            self.allocated(pointer, layout.size(), true);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if self.enabled.load(Ordering::Relaxed) {
            self.deallocs.fetch_add(1, Ordering::Relaxed);
            self.dealloc_bytes
                .fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { self.inner.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let enabled = self.request(size);
        if enabled {
            self.reallocs.fetch_add(1, Ordering::Relaxed);
        }
        let resized = unsafe { self.inner.realloc(pointer, layout, size) };
        if enabled {
            if resized.is_null() {
                self.realloc_failures.fetch_add(1, Ordering::Relaxed);
            } else {
                self.realloc_successes.fetch_add(1, Ordering::Relaxed);
                // Целевой requested size, а не прирост и не оценка живой кучи.
                self.realloc_success_bytes
                    .fetch_add(size as u64, Ordering::Relaxed);
            }
        }
        resized
    }
}
