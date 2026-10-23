use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use super::support::{
    FileHash, TestDir, assert_archive_hashes, binary_path, insert_expected_file,
    insert_expected_random_file, reserve_loopback_addr,
};

const DUMP_ID: &str = "process-crash";
const SPECULATIVE_BYTES: u64 = 8 * 1024 * 1024;
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug)]
enum Pairing {
    Push,
    Pull,
}

struct OwnedChild {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl OwnedChild {
    fn spawn(command: &mut Command, logs: &Path, label: &str) -> Self {
        let stdout = logs.join(format!("{label}.stdout"));
        let stderr = logs.join(format!("{label}.stderr"));
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

    fn diagnostics(&self) -> String {
        format!(
            "pid={} stdout={} stderr={}",
            self.child.id(),
            fs::read_to_string(&self.stdout).unwrap_or_default(),
            fs::read_to_string(&self.stderr).unwrap_or_default(),
        )
    }

    fn wait(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "child timed out: {}",
                self.diagnostics()
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn kill(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "child exited before kill: {}",
            self.diagnostics()
        );
        self.child.kill().unwrap();
        assert!(!self.wait(Duration::from_secs(10)).success());
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(Some(_))) {
            return;
        }
        // Завершаем только созданный здесь Child; системные списки процессов не нужны.
        let _ = self.child.kill();
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
        eprintln!("could not reap owned test child: {}", self.diagnostics());
    }
}

fn sender_command(source: &Path, addr: SocketAddr, pairing: Pairing) -> Command {
    let mut command = Command::new(binary_path());
    command
        .args([
            "sender",
            match pairing {
                Pairing::Push => "--connect",
                Pairing::Pull => "--listen",
            },
        ])
        .arg(addr.to_string())
        .arg("--source")
        .arg(source)
        .args([
            "--dump-id",
            DUMP_ID,
            "--level",
            "3",
            "--threads",
            "1",
            "--min-readers",
            "2",
            "--max-readers",
            "2",
            "--frame-target-mb",
            "1",
        ]);
    command
}

fn receiver_command(output: &Path, addr: SocketAddr, pairing: Pairing) -> Command {
    let mut command = Command::new(binary_path());
    command
        .args([
            "receiver",
            match pairing {
                Pairing::Push => "--listen",
                Pairing::Pull => "--connect",
            },
        ])
        .arg(addr.to_string())
        .arg("--data-dir")
        .arg(output)
        .args(["--max-sessions", "1"]);
    command
}

fn start_pair(
    source: &Path,
    output: &Path,
    logs: &Path,
    pairing: Pairing,
    phase: &str,
) -> (OwnedChild, OwnedChild) {
    let addr = reserve_loopback_addr();
    let mut sender_command = sender_command(source, addr, pairing);
    let mut receiver_command = receiver_command(output, addr, pairing);
    let wal_path = output.join(format!("{DUMP_ID}.wal.part"));
    let wal_len_before = fs::metadata(&wal_path).map_or(0, |metadata| metadata.len());
    let (listener_command, connector_command) = match pairing {
        Pairing::Push => (&mut receiver_command, &mut sender_command),
        Pairing::Pull => (&mut sender_command, &mut receiver_command),
    };
    let mut listener = OwnedChild::spawn(listener_command, logs, &format!("{phase}-listener"));
    let mut attempt = 0;
    let mut connector = OwnedChild::spawn(
        connector_command,
        logs,
        &format!("{phase}-connector-{attempt}"),
    );
    let started = Instant::now();
    loop {
        // Нельзя проверять готовность фиктивным TCP connect: sender --listen
        // принял бы его за настоящий первый канал. Повторяем только отказ connect.
        if let Some(status) = connector.child.try_wait().unwrap() {
            if status.success() {
                break;
            }
            let stderr = fs::read_to_string(&connector.stderr).unwrap();
            assert!(
                started.elapsed() < Duration::from_secs(10)
                    && stderr.contains("failed to connect to")
                    && listener.child.try_wait().unwrap().is_none(),
                "connector failed: {}; listener: {}",
                connector.diagnostics(),
                listener.diagnostics()
            );
            attempt += 1;
            thread::sleep(Duration::from_millis(5));
            connector = OwnedChild::spawn(
                connector_command,
                logs,
                &format!("{phase}-connector-{attempt}"),
            );
            continue;
        }
        if let Some(status) = listener.child.try_wait().unwrap() {
            assert!(
                status.success(),
                "listener failed: {}",
                listener.diagnostics()
            );
            break;
        }
        if fs::metadata(&wal_path).is_ok_and(|metadata| metadata.len() > wal_len_before)
            || output.join(format!("{DUMP_ID}.tar.zst")).exists()
        {
            break;
        }
        assert!(
            started.elapsed() < TRANSFER_TIMEOUT,
            "session startup timed out: {}; {}",
            listener.diagnostics(),
            connector.diagnostics()
        );
        thread::sleep(Duration::from_millis(1));
    }
    match pairing {
        Pairing::Push => (connector, listener),
        Pairing::Pull => (listener, connector),
    }
}

#[derive(Debug)]
struct Boundary {
    bundle: u64,
    archive_len: u64,
}

fn last_complete_boundary(path: &Path) -> Option<Boundary> {
    let content = match fs::read(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("cannot inspect WAL {}: {error}", path.display()),
    };
    let mut lines = content.split_inclusive(|byte| *byte == b'\n');
    let header = lines.next()?;
    if !header.ends_with(b"\n") {
        return None;
    }
    assert_eq!(header, b"fast-dump-v4-wal-1\n");
    let mut boundary = None;
    for line in lines {
        let Some(line) = line.strip_suffix(b"\n") else {
            break;
        };
        let fields = std::str::from_utf8(line)
            .unwrap()
            .split('\t')
            .collect::<Vec<_>>();
        assert_eq!(fields.len(), 8);
        assert_eq!(fields[0], "commit");
        boundary = Some(Boundary {
            bundle: fields[1].parse().unwrap(),
            archive_len: fields[4].parse().unwrap(),
        });
    }
    boundary
}

fn wait_for_speculative_tail(
    output: &Path,
    sender: &mut OwnedChild,
    receiver: &mut OwnedChild,
) -> Boundary {
    let deadline = Instant::now() + TRANSFER_TIMEOUT;
    let wal = output.join(format!("{DUMP_ID}.wal.part"));
    let part = output.join(format!("{DUMP_ID}.tar.zst.part"));
    loop {
        assert!(
            receiver.child.try_wait().unwrap().is_none(),
            "receiver exited before crash point: {}",
            receiver.diagnostics()
        );
        assert!(
            sender.child.try_wait().unwrap().is_none(),
            "sender exited before crash point: {}",
            sender.diagnostics()
        );
        if let Some(boundary) = last_complete_boundary(&wal)
            && fs::metadata(&part)
                .is_ok_and(|metadata| metadata.len() >= boundary.archive_len + SPECULATIVE_BYTES)
        {
            return boundary;
        }
        assert!(
            Instant::now() < deadline,
            "no committed WAL + speculative tail observed; sender: {}; receiver: {}",
            sender.diagnostics(),
            receiver.diagnostics()
        );
        thread::sleep(Duration::from_millis(1));
    }
}

fn read_prefix(path: &Path, length: u64) -> Vec<u8> {
    let mut prefix = Vec::new();
    fs::File::open(path)
        .unwrap()
        .take(length)
        .read_to_end(&mut prefix)
        .unwrap();
    assert_eq!(
        prefix.len() as u64,
        length,
        "archive shorter than WAL boundary"
    );
    prefix
}

fn fixture(source: &Path) -> BTreeMap<String, FileHash> {
    let mut expected = BTreeMap::new();
    for index in 0..16 {
        insert_expected_random_file(
            &mut expected,
            source,
            &format!("a-primer-{index:02}.bin"),
            1024 * 1024,
            101 + index,
        );
    }
    for index in 0..1024_u64 {
        let bytes = (0..4096_u64)
            .map(|offset| index.wrapping_mul(17).wrapping_add(offset * 31) as u8)
            .collect::<Vec<_>>();
        insert_expected_file(
            &mut expected,
            source,
            &format!("m-small/{:02}/{index:04}.bin", index / 32),
            &bytes,
        );
    }
    insert_expected_random_file(
        &mut expected,
        source,
        "z-streamed.bin",
        128 * 1024 * 1024,
        987654321,
    );
    expected
}

#[test]
#[ignore = "process-kill durability stress; run explicitly for release validation"]
fn receiver_process_kill_and_resume_preserves_archive_push_and_pull() {
    let source = TestDir::new();
    let expected = fixture(source.path());
    for pairing in [Pairing::Push, Pairing::Pull] {
        let output = TestDir::new();
        let logs = TestDir::new();
        let (mut sender, mut receiver) =
            start_pair(source.path(), output.path(), logs.path(), pairing, "crash");
        let observed = wait_for_speculative_tail(output.path(), &mut sender, &mut receiver);
        receiver.kill();
        assert!(
            !sender.wait(Duration::from_secs(20)).success(),
            "sender reported success after receiver kill"
        );
        let part = output.path().join(format!("{DUMP_ID}.tar.zst.part"));
        let final_archive = output.path().join(format!("{DUMP_ID}.tar.zst"));
        let boundary =
            last_complete_boundary(&output.path().join(format!("{DUMP_ID}.wal.part"))).unwrap();
        assert!(boundary.bundle >= observed.bundle && boundary.archive_len >= observed.archive_len);
        assert!(
            fs::metadata(&part).unwrap().len() > boundary.archive_len,
            "kill missed speculative tail: {pairing:?}"
        );
        assert!(
            !final_archive.exists(),
            "kill happened after finalization: {pairing:?}"
        );
        let prefix = read_prefix(&part, boundary.archive_len);
        drop(sender);
        drop(receiver);

        let (mut sender, mut receiver) =
            start_pair(source.path(), output.path(), logs.path(), pairing, "resume");
        let status = sender.wait(TRANSFER_TIMEOUT);
        assert!(
            status.success(),
            "resumed sender failed: {}",
            sender.diagnostics()
        );
        match pairing {
            Pairing::Pull => {
                assert!(
                    receiver.wait(TRANSFER_TIMEOUT).success(),
                    "resumed receiver failed: {}",
                    receiver.diagnostics()
                );
            }
            Pairing::Push => receiver.kill(),
        }
        assert_eq!(
            read_prefix(&final_archive, boundary.archive_len),
            prefix,
            "committed archive prefix changed: {pairing:?}"
        );
        assert_archive_hashes(&final_archive, &expected);
        assert!(!part.exists());
        assert!(!output.path().join(format!("{DUMP_ID}.wal.part")).exists());
        eprintln!(
            "{pairing:?}: killed after bundle {}, preserved {} compressed bytes, verified {} files",
            boundary.bundle,
            boundary.archive_len,
            expected.len()
        );
    }
}
