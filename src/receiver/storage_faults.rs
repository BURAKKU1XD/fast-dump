use std::cell::RefCell;
use std::fs::File;
use std::io::{self, Write};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Point {
    ArchiveWrite,
    ArchiveSync,
    WalAppend,
    WalSync,
    ArchiveRename,
    ArchiveParentSync,
    CleanupRemove,
    CleanupParentSync,
}

struct Fault {
    point: Point,
    successful_calls: usize,
    partial_bytes: usize,
}

thread_local! {
    // Ошибка принадлежит только потоку теста: параллельные сессии не затрагиваются.
    static FAULT: RefCell<Option<Fault>> = const { RefCell::new(None) };
}

pub(super) struct Guard;

impl Guard {
    pub(super) fn assert_consumed(&self) {
        FAULT.with(|slot| assert!(slot.borrow().is_none(), "storage fault was not exercised"));
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        FAULT.with(|slot| *slot.borrow_mut() = None);
    }
}

pub(super) fn arm(point: Point, successful_calls: usize, partial_bytes: usize) -> Guard {
    FAULT.with(|slot| {
        let mut slot = slot.borrow_mut();
        assert!(slot.is_none(), "nested storage faults are not supported");
        *slot = Some(Fault {
            point,
            successful_calls,
            partial_bytes,
        });
    });
    Guard
}

fn take(point: Point) -> Option<Fault> {
    FAULT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let fault = slot.as_mut()?;
        if fault.point != point {
            return None;
        }
        if fault.successful_calls != 0 {
            fault.successful_calls -= 1;
            return None;
        }
        slot.take()
    })
}

pub(super) fn check(point: Point) -> io::Result<()> {
    if take(point).is_some() {
        return Err(io::Error::other(format!(
            "injected storage fault: {point:?}"
        )));
    }
    Ok(())
}

pub(super) fn write_failure(point: Point, file: &mut File, bytes: &[u8]) -> io::Result<()> {
    if let Some(fault) = take(point) {
        assert!(
            fault.partial_bytes < bytes.len(),
            "fault must leave an incomplete write"
        );
        // Префикс действительно попадает в файл. Затем моделируем ENOSPC/
        // ERROR_DISK_FULL, а не успешную короткую запись, которую write_all повторил бы.
        file.write_all(&bytes[..fault.partial_bytes])?;
        #[cfg(windows)]
        let error = io::Error::from_raw_os_error(112);
        #[cfg(unix)]
        let error = io::Error::from_raw_os_error(28);
        #[cfg(not(any(windows, unix)))]
        let error = io::Error::other("injected disk full");
        return Err(error);
    }
    Ok(())
}
