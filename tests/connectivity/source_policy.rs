use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::support::*;

const CHILD_ENV: &str = "FAST_DUMP_TEST_PRIVATE_SOURCE_MOUNTS";
const TEST_NAME: &str = "connectivity::source_policy::linux_system_filesystem_mounts";

struct TestMount(PathBuf);

impl TestMount {
    fn bind(source: &str, target: &Path) -> Self {
        // В user namespace нельзя отделить заблокированные вложенные mounts:
        // рекурсивный bind сохраняет их и проверяет отсечение всего дерева.
        let option = if Path::new(source).is_dir() {
            "--rbind"
        } else {
            "--bind"
        };
        Self::mount(&[option, source], target)
    }

    fn mount(arguments: &[&str], target: &Path) -> Self {
        let result = Command::new("mount")
            .args(arguments)
            .arg(target)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "mount {}: {}",
            target.display(),
            String::from_utf8_lossy(&result.stderr)
        );
        Self(target.to_owned())
    }
}

impl Drop for TestMount {
    fn drop(&mut self) {
        // Отсоединяем целое дерево: вложенные mounts в user namespace могут быть
        // заблокированы для отдельного umount. TestDir затем видит исходный каталог.
        let result = Command::new("umount").arg("--lazy").arg(&self.0).status();
        if !result.is_ok_and(|status| status.success()) {
            eprintln!("failed to unmount test path {}", self.0.display());
            // Завершение дочернего namespace освободит mount; обход чужой ФС недопустим.
            std::process::exit(1);
        }
    }
}

#[test]
#[ignore = "requires Linux unshare with user/mount namespaces and mount/umount tools"]
fn linux_system_filesystem_mounts() {
    if std::env::var_os(CHILD_ENV).is_none() {
        // Монтирования существуют только в отдельном namespace дочернего процесса.
        let result = Command::new("unshare")
            .args([
                "--user",
                "--map-root-user",
                "--mount",
                "--propagation",
                "private",
            ])
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", TEST_NAME, "--ignored", "--nocapture"])
            .env(CHILD_ENV, "1")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "isolated mount test failed:\n{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        return;
    }

    let source = TestDir::new();
    let mut expected = BTreeMap::new();
    insert_expected_file(&mut expected, source.path(), "ordinary.txt", b"ordinary");
    insert_expected_file(&mut expected, source.path(), "proc/kcore", b"real file");
    insert_expected_file(&mut expected, source.path(), "sys/config", b"real config");
    for index in 0..96 {
        insert_expected_file(
            &mut expected,
            source.path(),
            &format!("small/{index:03}"),
            b"reader concurrency fixture",
        );
    }
    for name in ["kernel-state", "device-state", "ram-data"] {
        fs::create_dir(source.path().join(name)).unwrap();
    }
    fs::write(source.path().join("kernel-version"), []).unwrap();

    let _proc_mount = TestMount::bind("/proc", &source.path().join("kernel-state"));
    let _sys_mount = TestMount::bind("/sys", &source.path().join("device-state"));
    let _file_mount = TestMount::bind("/proc/version", &source.path().join("kernel-version"));
    let _tmpfs_mount = TestMount::mount(
        &["-t", "tmpfs", "-o", "size=8m", "tmpfs"],
        &source.path().join("ram-data"),
    );
    insert_expected_file(
        &mut expected,
        source.path(),
        "ram-data/payload",
        b"keep tmpfs",
    );

    for readers in [1, 4] {
        let dump_id = format!("system-fs-{readers}");
        let mut options = default_send_options(1024 * 1024);
        options.readers = readers;
        let result = run_push_roundtrip_with_options(source.path(), &dump_id, options);
        assert_archive_hashes(&result.receive_report.final_archive_path, &expected);
        assert_eq!(result.sender_report.terminal_skip_count, 3);
        assert_eq!(result.sender_profile.bundled_entries, expected.len() as u64);
        let issues = fs::read_to_string(
            result
                .receive_report
                .final_archive_path
                .with_file_name(format!("{dump_id}.tar.zst.issues.tsv")),
        )
        .unwrap();
        for path in ["kernel-state", "device-state"] {
            assert!(issues.contains(&format!("0\tunsupported_entry\t{path}\n")));
        }
        assert!(issues.contains("\tunsupported_entry\tkernel-version\n"));
        assert_eq!(issues.lines().count(), 4);
    }
}
