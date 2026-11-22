use std::collections::BTreeMap;
use std::fs;
use std::net::{SocketAddr, TcpListener};
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
        while !self
            .stderr()
            .rsplit_once('\n')
            .is_some_and(|(complete_lines, _)| complete_lines.contains(fragment))
        {
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

#[test]
fn occupied_listener_reports_the_address_for_each_cli_role() {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap().to_string();
    let work = TestDir::new();
    for role in ["relay", "sender", "receiver"] {
        let mut command = Command::new(binary_path());
        command.args(["--no-gui", role, "--listen", &address]);
        match role {
            "relay" => command.arg("--access"),
            "sender" => command
                .args(["--dump-id", "bind-check", "--source"])
                .arg(work.path()),
            "receiver" => command.arg("--data-dir").arg(work.path()),
            _ => unreachable!(),
        };
        let mut process = Process::spawn(command, work.path(), role);
        assert_eq!(process.wait().code(), Some(1), "{}", process.stderr());
        assert!(
            process.stderr().contains(&format!(
                "Cannot listen on {address}: port is already in use"
            )),
            "{}",
            process.stderr()
        );
        assert!(process.stdout().is_empty());
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

#[test]
fn relay_command_pairs_two_outbound_clients_and_writes_valid_archive() {
    let source = TestDir::new();
    let output = TestDir::new();
    let logs = TestDir::new();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/relay");
    for name in ["ca.pem", "server.pem", "server-key.pem"] {
        fs::copy(fixture.join(name), logs.path().join(name)).unwrap();
    }
    fs::write(logs.path().join("sender.key"), "11".repeat(32)).unwrap();
    fs::write(logs.path().join("receiver.key"), "22".repeat(32)).unwrap();
    fs::write(
        logs.path().join("relay.toml"),
        r#"certificate = "server.pem"
private_key = "server-key.pem"

[rooms.backup]
sender_key_file = "sender.key"
receiver_key_file = "receiver.key"
"#,
    )
    .unwrap();
    let mut expected = BTreeMap::new();
    insert_expected_random_file(
        &mut expected,
        source.path(),
        "payload.bin",
        2 * 1024 * 1024,
        71,
    );

    let mut command = Command::new(binary_path());
    command
        .args(["relay", "--listen", "127.0.0.1:0", "--config"])
        .arg(logs.path().join("relay.toml"));
    let mut relay = Process::spawn(command, logs.path(), "relay");
    let prefix = "Relay listening on ";
    relay.wait_for_stderr(prefix);
    let listening = relay.stderr();
    let address = listening
        .lines()
        .find_map(|line| line.strip_prefix(prefix))
        .unwrap();
    let client = |role: &str| {
        let mut cli = Command::new(binary_path());
        cli.args([
            "--no-gui",
            role,
            "--relay",
            address,
            "--room",
            "backup",
            "--relay-token-file",
        ])
        .arg(logs.path().join(format!("{role}.key")))
        .arg("--relay-ca-file")
        .arg(logs.path().join("ca.pem"));
        cli
    };

    let mut send = client("sender");
    send.args(["--dump-id", "relay-cli", "--threads", "1", "--source"])
        .arg(source.path());
    let mut sender = Process::spawn(send, logs.path(), "sender");
    sender.wait_for_stderr("Waiting for the other peer through the relay");
    let mut receive = client("receiver");
    receive.arg("--data-dir").arg(output.path());
    let mut receiver = Process::spawn(receive, logs.path(), "receiver");

    assert!(sender.wait().success(), "{}", sender.stderr());
    assert!(receiver.wait().success(), "{}", receiver.stderr());
    assert!(sender.stdout().contains("status=clean;"));
    assert!(receiver.stdout().contains("terminal_skips=0;"));
    assert_archive_hashes(&output.path().join("relay-cli.tar.zst"), &expected);
    assert!(relay.child.try_wait().unwrap().is_none());
}
