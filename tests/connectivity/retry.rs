use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use super::support::{
    TestDir, assert_archive_hashes, binary_path, insert_expected_file, insert_expected_random_file,
    reserve_loopback_addr,
};

struct Process {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl Process {
    fn spawn(mut command: Command, logs: &Path, name: &str) -> Self {
        let stdout = logs.join(format!("{name}.stdout"));
        let stderr = logs.join(format!("{name}.stderr"));
        let child = command
            .stdin(Stdio::null())
            .stdout(fs::File::create(&stdout).unwrap())
            .stderr(fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        Self {
            child,
            stdout,
            stderr,
        }
    }

    fn stderr(&self) -> String {
        fs::read_to_string(&self.stderr).unwrap()
    }

    fn stdout(&self) -> String {
        fs::read_to_string(&self.stdout).unwrap()
    }

    fn wait_for_stderr(&mut self, fragment: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !self.stderr().contains(fragment) {
            assert!(
                self.child.try_wait().unwrap().is_none() && Instant::now() < deadline,
                "process did not emit {fragment:?}: {}",
                self.stderr(),
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait(&mut self) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "process timed out: {}",
                self.stderr()
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // При любом assert освобождаем только запущенный этим тестом процесс.
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn sender(source: &Path, addr: SocketAddr, dump_id: &str, token: Option<&str>) -> Command {
    let mut command = Command::new(binary_path());
    command
        .args([
            "--no-gui",
            "--retries",
            "3",
            "--retry-delay-secs",
            "1",
            "sender",
            "--connect",
        ])
        .arg(addr.to_string())
        .arg("--source")
        .arg(source)
        .args([
            "--dump-id",
            dump_id,
            "--threads",
            "1",
            "--min-readers",
            "1",
            "--max-readers",
            "1",
            "--frame-target-mb",
            "1",
        ]);
    if let Some(token) = token {
        command.args(["--token", token]);
    }
    command
}

fn receiver(data_dir: &Path, addr: SocketAddr, token: Option<&str>) -> Command {
    let mut command = Command::new(binary_path());
    command
        .args(["--no-gui", "--debug", "receiver", "--listen"])
        .arg(addr.to_string())
        .arg("--data-dir")
        .arg(data_dir)
        .args(["--max-sessions", "1"]);
    if let Some(token) = token {
        command.args(["--token", token]);
    }
    command
}

#[test]
fn sender_retries_refused_connection_then_completes_after_receiver_starts() {
    let source = TestDir::new();
    let output = TestDir::new();
    let logs = TestDir::new();
    let mut expected = BTreeMap::new();
    insert_expected_file(
        &mut expected,
        source.path(),
        "hello.txt",
        b"retry succeeded",
    );
    insert_expected_random_file(
        &mut expected,
        source.path(),
        "nested/payload.bin",
        2 * 1024 * 1024,
        17,
    );
    let addr = reserve_loopback_addr();
    let dump_id = "delayed-receiver";
    let mut sender = Process::spawn(
        sender(source.path(), addr, dump_id, None),
        logs.path(),
        "sender",
    );

    // Создаём receiver только после подтверждённого отказа connect, без гонки
    // между запуском процесса и предполагаемой фиксированной задержкой теста.
    sender.wait_for_stderr("Retry 1/3");
    let mut receiver = Process::spawn(receiver(output.path(), addr, None), logs.path(), "receiver");
    assert!(sender.wait().success(), "{}", sender.stderr());
    assert!(sender.stdout().contains("status=clean"));
    assert!(sender.stdout().contains("files_sent=2"));
    assert!(!sender.stdout().contains('\x1b'));
    assert!(!sender.stderr().contains('\x1b'));
    assert_archive_hashes(&output.path().join(format!("{dump_id}.tar.zst")), &expected);
    assert!(
        receiver.child.try_wait().unwrap().is_none(),
        "listener exited after success"
    );
}

#[test]
fn receiver_retries_refused_connection_then_completes_after_sender_starts() {
    let source = TestDir::new();
    let output = TestDir::new();
    let logs = TestDir::new();
    let mut expected = BTreeMap::new();
    insert_expected_file(
        &mut expected,
        source.path(),
        "hello.txt",
        b"receiver retry succeeded",
    );
    insert_expected_random_file(
        &mut expected,
        source.path(),
        "nested/payload.bin",
        2 * 1024 * 1024,
        23,
    );
    let addr = reserve_loopback_addr();
    let dump_id = "delayed-sender";
    let mut connect = Command::new(binary_path());
    connect
        .args([
            "--no-gui",
            "--retries",
            "3",
            "--retry-delay-secs",
            "1",
            "receiver",
            "--connect",
        ])
        .arg(addr.to_string())
        .arg("--data-dir")
        .arg(output.path());
    let mut receiver = Process::spawn(connect, logs.path(), "receiver");
    receiver.wait_for_stderr("Retry 1/3");

    let mut listen = Command::new(binary_path());
    listen
        .args(["--no-gui", "sender", "--listen"])
        .arg(addr.to_string())
        .arg("--source")
        .arg(source.path())
        .args([
            "--dump-id",
            dump_id,
            "--threads",
            "1",
            "--min-readers",
            "1",
            "--max-readers",
            "1",
            "--frame-target-mb",
            "1",
        ]);
    let mut sender = Process::spawn(listen, logs.path(), "sender");
    assert!(receiver.wait().success(), "{}", receiver.stderr());
    assert!(sender.wait().success(), "{}", sender.stderr());
    assert!(receiver.stdout().contains("terminal_skips=0"));
    assert!(receiver.stdout().contains("archive="));
    assert!(sender.stdout().contains("status=clean"));
    assert!(sender.stdout().contains("files_sent=2"));
    assert!(!receiver.stdout().contains('\x1b'));
    assert!(!receiver.stderr().contains('\x1b'));
    assert_archive_hashes(&output.path().join(format!("{dump_id}.tar.zst")), &expected);
}

#[test]
fn explicit_auth_rejection_does_not_retry_and_listener_accepts_the_next_sender() {
    let source = TestDir::new();
    let output = TestDir::new();
    let logs = TestDir::new();
    let mut expected = BTreeMap::new();
    insert_expected_file(
        &mut expected,
        source.path(),
        "hello.txt",
        b"authorized transfer",
    );
    let addr = reserve_loopback_addr();
    let dump_id = "auth-rejection";
    let mut receiver = Process::spawn(
        receiver(output.path(), addr, Some("secret")),
        logs.path(),
        "receiver",
    );
    receiver.wait_for_stderr("v4 receiver listener entering serve loop:");

    let mut rejected = Process::spawn(
        sender(source.path(), addr, dump_id, Some("wrong")),
        logs.path(),
        "rejected",
    );
    assert_eq!(rejected.wait().code(), Some(1));
    assert!(
        rejected.stderr().contains("bad token"),
        "{}",
        rejected.stderr()
    );
    assert!(
        !rejected.stderr().contains("Retry "),
        "{}",
        rejected.stderr()
    );
    assert!(rejected.stdout().is_empty());
    assert!(!output.path().join(format!("{dump_id}.tar.zst")).exists());
    assert!(receiver.child.try_wait().unwrap().is_none());

    let mut accepted = Process::spawn(
        sender(source.path(), addr, dump_id, Some("secret")),
        logs.path(),
        "accepted",
    );
    assert!(accepted.wait().success(), "{}", accepted.stderr());
    assert!(
        !accepted.stderr().contains("Retry "),
        "{}",
        accepted.stderr()
    );
    assert_archive_hashes(&output.path().join(format!("{dump_id}.tar.zst")), &expected);
    assert!(receiver.child.try_wait().unwrap().is_none());
}
