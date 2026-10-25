use std::fs::File;
use std::io;
use std::path::Path;

use crate::runtime::SessionControl;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExcludedFilesystem {
    name: &'static str,
    reason: &'static str,
}

impl ExcludedFilesystem {
    pub(crate) const fn name(self) -> &'static str {
        self.name
    }

    pub(crate) const fn reason(self) -> &'static str {
        self.reason
    }

    #[cfg(test)]
    pub(crate) fn procfs_for_test() -> Self {
        classify(0x9fa0).unwrap()
    }
}

// Значения Linux UAPI magic.h; fusectl, rpc_pipefs и nfsd объявлены
// внутри соответствующих драйверов. Сравниваем ровно младшие 32 бита:
// signed f_type на 32-битной платформе не должен менять классификацию.
#[cfg(any(target_os = "linux", test))]
fn classify(magic: u32) -> Option<ExcludedFilesystem> {
    let (name, reason) = match magic {
        0x0000_9fa0 => ("proc", "process and kernel memory interface"),
        0x6265_6572 => ("sysfs", "kernel device and attribute interface"),
        0x6462_6720 => ("debugfs", "kernel debugging interface"),
        0x7472_6163 => ("tracefs", "kernel tracing interface"),
        0x0027_e0eb => ("cgroup", "control group interface"),
        0x6367_7270 => ("cgroup2", "control group interface"),
        0x0765_5821 => ("resctrl", "hardware resource control interface"),
        0x6265_6570 => ("configfs", "kernel configuration interface"),
        0x4249_4e4d => ("binfmt_misc", "binary format registration interface"),
        0x7363_6673 => ("securityfs", "kernel security interface"),
        0xf97c_ff8c => ("selinuxfs", "SELinux policy interface"),
        0x4341_5d53 => ("smackfs", "Smack policy interface"),
        0xcafe_4a11 => ("bpf", "pinned kernel BPF objects"),
        0x6e73_6673 => ("nsfs", "kernel namespace handles"),
        0xde5e_81e4 => ("efivarfs", "firmware variable interface"),
        0x1980_0202 => ("mqueue", "POSIX message queue interface"),
        0x0000_1cd1 => ("devpts", "pseudo-terminal interface"),
        0x6573_5543 => ("fusectl", "FUSE connection control interface"),
        0x6759_6969 => ("rpc_pipefs", "kernel RPC communication interface"),
        0x6e66_7364 => ("nfsd", "NFS server control interface"),
        _ => return None,
    };
    Some(ExcludedFilesystem { name, reason })
}

pub(crate) fn excluded_directory(path: &Path) -> io::Result<Option<ExcludedFilesystem>> {
    #[cfg(test)]
    if let Some(result) = test_hooks::directory_result(path) {
        return result;
    }
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source path contains NUL"))?;
        filesystem_type(|output| {
            // CString и выходной statfs живут до возврата синхронного syscall.
            unsafe { libc::statfs(path.as_ptr(), output) }
        })
        .map(classify)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        Ok(None)
    }
}

pub(crate) fn excluded_file(file: &File) -> io::Result<Option<ExcludedFilesystem>> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;

        filesystem_type(|output| {
            // Проверяем именно открытый handle, даже если pathname уже заменён.
            unsafe { libc::fstatfs(file.as_raw_fd(), output) }
        })
        .map(classify)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        Ok(None)
    }
}

#[cfg(target_os = "linux")]
fn filesystem_type(mut query: impl FnMut(*mut libc::statfs) -> libc::c_int) -> io::Result<u32> {
    loop {
        let mut output = std::mem::MaybeUninit::<libc::statfs>::uninit();
        if query(output.as_mut_ptr()) == 0 {
            // Успешный statfs/fstatfs инициализирует всю структуру.
            return Ok(unsafe { output.assume_init() }.f_type as u32);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub(crate) fn log_exclusion(
    control: Option<&SessionControl>,
    relative_path: &str,
    filesystem: ExcludedFilesystem,
) {
    let message = format!(
        "Excluded {}: {} ({}); pending durable skip acknowledgement",
        crate::progress::display_text(relative_path, 512),
        filesystem.name(),
        filesystem.reason(),
    );
    if let Some(control) = control {
        control.log(log::Level::Warn, &message);
    } else {
        log::warn!("{message}");
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};

    type Outcome = Result<ExcludedFilesystem, io::ErrorKind>;
    static DIRECTORIES: OnceLock<Mutex<BTreeMap<PathBuf, Outcome>>> = OnceLock::new();

    pub(crate) struct DirectoryOverride(PathBuf);

    impl Drop for DirectoryOverride {
        fn drop(&mut self) {
            DIRECTORIES.get().unwrap().lock().unwrap().remove(&self.0);
        }
    }

    pub(crate) fn directory(path: &Path, outcome: Outcome) -> DirectoryOverride {
        let previous = DIRECTORIES
            .get_or_init(Mutex::default)
            .lock()
            .unwrap()
            .insert(path.to_owned(), outcome);
        assert!(
            previous.is_none(),
            "duplicate directory classifier override"
        );
        DirectoryOverride(path.to_owned())
    }

    pub(super) fn directory_result(path: &Path) -> Option<io::Result<Option<ExcludedFilesystem>>> {
        DIRECTORIES
            .get_or_init(Mutex::default)
            .lock()
            .unwrap()
            .get(path)
            .copied()
            .map(|result| result.map(Some).map_err(io::Error::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_system_interfaces_are_excluded() {
        let denied = [
            0x9fa0, 0x62656572, 0x64626720, 0x74726163, 0x27e0eb, 0x63677270, 0x7655821,
            0x62656570, 0x42494e4d, 0x73636673, 0xf97cff8c, 0x43415d53, 0xcafe4a11, 0x6e736673,
            0xde5e81e4, 0x19800202, 0x1cd1, 0x65735543, 0x67596969, 0x6e667364,
        ];
        for magic in denied {
            let filesystem = classify(magic).unwrap();
            assert!(!filesystem.name().is_empty());
            assert!(!filesystem.reason().is_empty());
        }
        // tmpfs, ramfs, overlay, обычный FUSE, NFS, ext4 и неизвестные FS несут данные.
        for magic in [
            0x01021994, 0x858458f6, 0x794c7630, 0x65735546, 0x6969, 0xef53, 0,
        ] {
            assert_eq!(classify(magic), None);
        }
    }

    #[test]
    fn ordinary_filesystem_names_do_not_change_policy() {
        let source = crate::tests::TestDir::new();
        let directory = source.path().join("proc");
        std::fs::create_dir(&directory).unwrap();
        let file = File::create(directory.join("kcore")).unwrap();
        assert_eq!(excluded_directory(&directory).unwrap(), None);
        assert_eq!(excluded_file(&file).unwrap(), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn procfs_is_detected_by_directory_and_open_handle() {
        assert_eq!(
            excluded_directory(Path::new("/proc"))
                .unwrap()
                .unwrap()
                .name(),
            "proc"
        );
        let file = File::open("/proc/version").unwrap();
        assert_eq!(excluded_file(&file).unwrap().unwrap().name(), "proc");
        assert!(excluded_directory(Path::new("/proc/fast-dump-missing-source")).is_err());
    }
}
