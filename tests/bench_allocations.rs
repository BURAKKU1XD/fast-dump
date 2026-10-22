#![cfg(feature = "bench-allocations")]

#[path = "../benches/support/allocation_counter.rs"]
mod allocation_counter;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use allocation_counter::CountingAllocator;

fn layout(bytes: usize) -> Layout {
    Layout::from_size_align(bytes, 8).unwrap()
}

#[test]
fn measurement_scope_resets_operations_without_assuming_zero_live_allocations() {
    let allocator = CountingAllocator::new(System);
    // Прямые вызовы локального allocator не затрагивают allocator test runner.
    unsafe {
        let old = allocator.alloc(layout(32));
        assert!(!old.is_null());
        allocator.start();
        let payload = allocator.alloc(layout(64));
        let zeroed = allocator.alloc_zeroed(layout(16));
        assert!(!payload.is_null() && !zeroed.is_null());
        payload.write_bytes(0x5a, 64);
        assert_eq!(std::slice::from_raw_parts(zeroed, 16), &[0; 16]);
        let payload = allocator.realloc(payload, layout(64), 96);
        assert!(!payload.is_null());
        assert_eq!(std::slice::from_raw_parts(payload, 64), &[0x5a; 64]);
        allocator.dealloc(old, layout(32));
        let first = allocator.stop();
        assert_eq!(
            (first.allocs, first.alloc_bytes, first.reallocs),
            (3, 176, 1)
        );
        assert_eq!((first.alloc_successes, first.alloc_success_bytes), (2, 80));
        assert_eq!(
            (first.realloc_successes, first.realloc_success_bytes),
            (1, 96)
        );
        assert_eq!(
            (first.deallocs, first.dealloc_bytes, first.zeroed_bytes),
            (1, 32, 16)
        );

        let outside = allocator.alloc(layout(8));
        assert!(!outside.is_null());
        allocator.dealloc(outside, layout(8));
        assert_eq!(allocator.stop().allocs, first.allocs);

        allocator.start();
        let payload = allocator.realloc(payload, layout(96), 48);
        assert!(!payload.is_null());
        assert_eq!(std::slice::from_raw_parts(payload, 48), &[0x5a; 48]);
        allocator.dealloc(payload, layout(48));
        allocator.dealloc(zeroed, layout(16));
        let second = allocator.stop();
        assert_eq!(
            (second.allocs, second.alloc_bytes, second.reallocs),
            (1, 48, 1)
        );
        assert_eq!(
            (
                second.alloc_successes,
                second.alloc_success_bytes,
                second.zeroed_bytes
            ),
            (0, 0, 0)
        );
        assert_eq!(
            (second.realloc_successes, second.realloc_success_bytes),
            (1, 48)
        );
        assert_eq!((second.deallocs, second.dealloc_bytes), (2, 64));
        assert_eq!((second.alloc_failures, second.realloc_failures), (0, 0));
    }
}

struct FallibleSystem(Arc<AtomicBool>);

unsafe impl GlobalAlloc for FallibleSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if self.0.load(Ordering::Relaxed) {
            std::ptr::null_mut()
        } else {
            unsafe { System.alloc(layout) }
        }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if self.0.load(Ordering::Relaxed) {
            std::ptr::null_mut()
        } else {
            unsafe { System.alloc_zeroed(layout) }
        }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if self.0.load(Ordering::Relaxed) {
            std::ptr::null_mut()
        } else {
            unsafe { System.realloc(pointer, layout, size) }
        }
    }
}

#[test]
fn failed_requests_remain_legacy_requests_and_do_not_become_successes() {
    let fail = Arc::new(AtomicBool::new(false));
    let allocator = CountingAllocator::new(FallibleSystem(fail.clone()));
    allocator.start();
    unsafe {
        let original = allocator.alloc(layout(32));
        assert!(!original.is_null());
        original.write_bytes(0x35, 32);
        fail.store(true, Ordering::Relaxed);
        assert!(allocator.alloc(layout(64)).is_null());
        assert!(allocator.alloc_zeroed(layout(8)).is_null());
        assert!(allocator.realloc(original, layout(32), 128).is_null());
        // Неудачный realloc оставляет исходный allocation у вызывающего.
        assert_eq!(std::slice::from_raw_parts(original, 32), &[0x35; 32]);
        fail.store(false, Ordering::Relaxed);
        let resized = allocator.realloc(original, layout(32), 64);
        assert!(!resized.is_null());
        allocator.dealloc(resized, layout(64));
    }
    let counts = allocator.stop();
    assert_eq!(
        (counts.allocs, counts.alloc_bytes, counts.reallocs),
        (5, 296, 2)
    );
    assert_eq!(
        (counts.alloc_successes, counts.alloc_success_bytes),
        (1, 32)
    );
    assert_eq!(
        (counts.realloc_successes, counts.realloc_success_bytes),
        (1, 64)
    );
    assert_eq!((counts.alloc_failures, counts.realloc_failures), (2, 1));
    assert_eq!(
        (counts.deallocs, counts.dealloc_bytes, counts.zeroed_bytes),
        (1, 64, 0)
    );
}

#[test]
fn joined_workers_share_one_measurement_without_test_runner_allocations() {
    let allocator = Arc::new(CountingAllocator::new(System));
    allocator.start();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let allocator = &allocator;
            scope.spawn(move || {
                for _ in 0..8 {
                    unsafe {
                        let pointer = allocator.alloc(layout(64));
                        assert!(!pointer.is_null());
                        pointer.write_bytes(0xa5, 64);
                        allocator.dealloc(pointer, layout(64));
                    }
                }
            });
        }
    });
    let counts = allocator.stop();
    assert_eq!(
        (counts.allocs, counts.alloc_bytes, counts.reallocs),
        (32, 2048, 0)
    );
    assert_eq!(
        (counts.alloc_successes, counts.alloc_success_bytes),
        (32, 2048)
    );
    assert_eq!((counts.deallocs, counts.dealloc_bytes), (32, 2048));
}
