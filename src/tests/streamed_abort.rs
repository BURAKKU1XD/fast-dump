use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Cursor, Read, Write};
use std::path::PathBuf;
use std::rc::Rc;
use std::thread;
use std::time::{Duration, Instant};

use super::{TestDir, read_archive_entries, spawn_v4_receiver_once};
use crate::network::protocol::wire::{self, Message};
use crate::network::transport::connect_once;
use crate::receiver::ReceiverSession;
use crate::role::Role;
use crate::sender::{self, BundlePlanOptions, test_hooks};

const MIB: usize = 1024 * 1024;

#[derive(Default)]
struct TransferTrace {
    payload_bytes: BTreeMap<(u64, u32), u64>,
    prior_commit_sent: bool,
    prior_ack_read: bool,
    prior_inflight_at_abort: bool,
    aborted_payload_bytes: u64,
    archive_len_before_abort: u64,
    abort_ack_read: bool,
    stopped_before_retry: bool,
}

#[derive(Default)]
struct PacketCapture {
    pending: Vec<u8>,
}

impl PacketCapture {
    fn push(&mut self, bytes: &[u8]) -> Vec<Message> {
        self.pending.extend_from_slice(bytes);
        let mut messages = Vec::new();
        while self.pending.len() >= 12 {
            let body_len = u32::from_le_bytes(self.pending[8..12].try_into().unwrap()) as usize;
            let packet_len = 12 + body_len;
            if self.pending.len() < packet_len {
                break;
            }
            messages
                .push(wire::read_message(&mut Cursor::new(&self.pending[..packet_len])).unwrap());
            self.pending.drain(..packet_len);
        }
        messages
    }
}

struct TracedData<T> {
    inner: T,
    sent: PacketCapture,
    trace: Rc<RefCell<TransferTrace>>,
}

impl<T: Write> Write for TracedData<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        // Считаем только целые пакеты, уже принятые настоящим TCP stream.
        for message in self.sent.push(&bytes[..written]) {
            let Message::BundleData(data) = message else {
                panic!("unexpected message on data channel");
            };
            *self
                .trace
                .borrow_mut()
                .payload_bytes
                .entry((data.bundle_no, data.attempt_no))
                .or_default() += data.bytes.len() as u64;
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

struct TracedControl<T> {
    inner: T,
    sent: PacketCapture,
    received: PacketCapture,
    archive_part: PathBuf,
    trace: Rc<RefCell<TransferTrace>>,
}

impl<T: Write> Write for TracedControl<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let abort_ack_read = self.trace.borrow().abort_ack_read;
        if abort_ack_read {
            self.trace.borrow_mut().stopped_before_retry = true;
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "test disconnect after payload abort acknowledgement",
            ));
        }
        // Control packets малы: отправляем каждый целиком, чтобы поставить
        // барьер перед Abort, не завися от разбиения Write на header и body.
        for message in self.sent.push(bytes) {
            match &message {
                Message::BundleCommit(commit) if commit.descriptor.bundle_no == 1 => {
                    self.trace.borrow_mut().prior_commit_sent = true;
                }
                Message::BundleAbort(abort) => {
                    assert_eq!((abort.bundle_no, abort.attempt_no), (2, 1));
                    let mut trace = self.trace.borrow_mut();
                    trace.prior_inflight_at_abort =
                        trace.prior_commit_sent && !trace.prior_ack_read;
                    trace.aborted_payload_bytes = trace.payload_bytes[&(2, 1)];
                    assert!(trace.aborted_payload_bytes > wire::MAX_DATA_CHUNK_BYTES as u64);
                    let expected_len = trace.payload_bytes[&(1, 1)] + trace.aborted_payload_bytes;
                    // Проверяем реальный staged suffix: одного размера fixture
                    // и даже успешного write в socket недостаточно для rollback.
                    let deadline = Instant::now() + Duration::from_secs(10);
                    loop {
                        let current_len = File::open(&self.archive_part)
                            .and_then(|file| file.metadata())?
                            .len();
                        if current_len >= expected_len {
                            trace.archive_len_before_abort = current_len;
                            break;
                        }
                        if Instant::now() >= deadline {
                            return Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "receiver did not stage the transmitted payload before abort",
                            ));
                        }
                        thread::sleep(Duration::from_millis(1));
                    }
                }
                _ => {}
            }
            wire::write_message(&mut self.inner, &message).map_err(io::Error::other)?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<T: Read> Read for TracedControl<T> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(bytes)?;
        for message in self.received.push(&bytes[..read]) {
            match message {
                Message::BundleAck(ack) if ack.bundle_no == 1 => {
                    self.trace.borrow_mut().prior_ack_read = true;
                }
                Message::BundleAbortAck(ack) => {
                    assert_eq!((ack.bundle_no, ack.attempt_no), (2, 1));
                    self.trace.borrow_mut().abort_ack_read = true;
                }
                _ => {}
            }
        }
        Ok(read)
    }
}

fn incompressible_bytes(size: usize) -> Vec<u8> {
    let mut bytes = vec![0; size];
    let mut state = 0x92a1_b4c3_d5e6_f708_u64;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    bytes
}

#[test]
fn streamed_abort_after_received_payload_resumes_without_losing_prior_bundle() {
    let _guard = test_hooks::lock();
    test_hooks::clear();
    let source = TestDir::new();
    let data = TestDir::new();
    let prior = b"prior bundle must survive".repeat(128);
    let large = incompressible_bytes(16 * MIB);
    let tail = b"tail after resumed large file".to_vec();
    fs::write(source.path().join("a-prior.bin"), &prior).unwrap();
    let large_path = source.path().join("b-large.bin");
    fs::write(&large_path, &large).unwrap();
    fs::write(source.path().join("c-tail.bin"), &tail).unwrap();
    test_hooks::force_short_read_after_next_open(&large_path, (12 * MIB) as u64);
    let options = BundlePlanOptions {
        bundle_target_bytes: MIB,
        readers: 1,
        ..BundlePlanOptions::default()
    };
    let dump_id = "streamed-payload-abort-resume";
    let (addr, receiver) = spawn_v4_receiver_once(data.path());
    let mut session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    session
        .control_stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    session
        .data_stream
        .set_write_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let trace = Rc::new(RefCell::new(TransferTrace::default()));
    let archive_part = data.path().join(format!("{dump_id}.tar.zst.part"));
    {
        let mut control = TracedControl {
            inner: &mut session.control_stream,
            sent: PacketCapture::default(),
            received: PacketCapture::default(),
            archive_part: archive_part.clone(),
            trace: Rc::clone(&trace),
        };
        let mut payload = TracedData {
            inner: &mut session.data_stream,
            sent: PacketCapture::default(),
            trace: Rc::clone(&trace),
        };
        let error = sender::send_dump(
            &mut control,
            &mut payload,
            source.path().to_path_buf(),
            dump_id,
            options,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("test disconnect after payload abort"));
    }
    drop(session);
    assert!(receiver.join().unwrap().is_err());
    let trace = trace.borrow();
    assert!(trace.prior_inflight_at_abort);
    assert!(trace.abort_ack_read && trace.stopped_before_retry);
    assert!(trace.aborted_payload_bytes >= (2 * wire::MAX_DATA_CHUNK_BYTES) as u64);
    let prior_compressed_bytes = trace.payload_bytes[&(1, 1)];
    assert_eq!(
        trace.archive_len_before_abort,
        prior_compressed_bytes + trace.aborted_payload_bytes
    );
    assert!(!trace.payload_bytes.contains_key(&(2, 2)));

    let recovered = ReceiverSession::open(data.path().to_path_buf(), dump_id, 3).unwrap();
    assert_eq!(recovered.resume_state().last_committed_bundle_no, 1);
    assert_eq!(recovered.resume_state().archive_len, prior_compressed_bytes);
    assert_eq!(
        fs::metadata(&archive_part).unwrap().len(),
        prior_compressed_bytes
    );
    drop(recovered);
    test_hooks::clear();

    let (addr, receiver) = spawn_v4_receiver_once(data.path());
    let mut session = connect_once(Role::Sender, addr, Some(dump_id), None).unwrap();
    let sent = sender::send_dump(
        &mut session.control_stream,
        &mut session.data_stream,
        source.path().to_path_buf(),
        dump_id,
        options,
    )
    .unwrap();
    let received = receiver.join().unwrap().unwrap();
    assert_eq!(sent.bundles_sent, 2);
    assert_eq!(sent.final_bundle_no, 3);
    assert_eq!(sent.terminal_skip_count, 0);
    let entries = read_archive_entries(&received.final_archive_path);
    assert!(entries.iter().all(|entry| entry.entry_type.is_file()));
    let expected = [
        ("a-prior.bin", prior),
        ("b-large.bin", large),
        ("c-tail.bin", tail),
    ];
    assert_eq!(entries.len(), expected.len());
    for (entry, (path, contents)) in entries.into_iter().zip(expected) {
        assert_eq!(entry.path, path);
        assert!(entry.bytes == contents, "archive contents differ: {path}");
    }
}
