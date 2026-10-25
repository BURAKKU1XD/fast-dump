use std::fs;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use crate::network::transport::{BoundListener, connect_once};
use crate::progress::ProgressHub;
use crate::receiver::receive_dump_profile_controlled;
use crate::role::Role;
use crate::runtime::RunControl;
use crate::tests::{ArchiveEntrySummary, TestDir, read_archive_entries};

use super::{BundlePlanOptions, send_dump_controlled};

const REPETITIONS: usize = 5;

fn random_bytes(size: usize, state: &mut u64) -> Vec<u8> {
    let mut bytes = vec![0; size];
    for chunk in bytes.chunks_mut(8) {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    bytes
}

fn fixture(files: usize, bytes_per_file: usize) -> (TestDir, Vec<ArchiveEntrySummary>) {
    let source = TestDir::new();
    let mut state = 0x83b4_15e9_2d71_c6af;
    let expected = (0..files)
        .map(|index| {
            let entry = ArchiveEntrySummary {
                path: format!("{index:04}.bin"),
                entry_type: tar::EntryType::Regular,
                bytes: random_bytes(bytes_per_file, &mut state),
                link_name: None,
            };
            fs::write(source.path().join(&entry.path), &entry.bytes).unwrap();
            entry
        })
        .collect();
    (source, expected)
}

fn sample(source: &TestDir, expected: &[ArchiveEntrySummary], observed: bool) -> Duration {
    let output = TestDir::new();
    let hub = observed.then(|| Arc::new(ProgressHub::new()));
    let run = RunControl::new(hub.clone());
    let sender = run.session(Role::Sender, "observation-perf", "loopback");
    let receiver = run.session(Role::Receiver, "observation-perf", "loopback");
    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let output_path = output.path().to_owned();

    // Подключение, создание fixtures, проверка архива и остановка observer
    // находятся за пределами измерения. Настройки I/O одинаковы в обоих режимах.
    let (duration, received) = thread::scope(|scope| {
        let (receiver_ready, receive_ready) = mpsc::sync_channel(0);
        let (begin, beginning) = mpsc::sync_channel(0);
        let receive_worker = scope.spawn(move || {
            let session = listener.accept(Role::Receiver, None, None).unwrap();
            let _control = receiver.register(&session.control_stream).unwrap();
            let _data = receiver.register(&session.data_stream).unwrap();
            receiver_ready.send(()).unwrap();
            beginning.recv().unwrap();
            receive_dump_profile_controlled(
                session.control_stream,
                session.data_stream,
                output_path,
                "observation-perf",
                &receiver,
            )
        });
        let mut session =
            connect_once(Role::Sender, address, Some("observation-perf"), None).unwrap();
        let _control = sender.register(&session.control_stream).unwrap();
        let _data = sender.register(&session.data_stream).unwrap();
        receive_ready.recv().unwrap();
        let (stop, stopping) = mpsc::channel();
        let (ready, started) = mpsc::sync_channel(0);
        let (start_observer, observe_started) = mpsc::sync_channel(0);
        let observer = hub.as_ref().map(|hub| {
            hub.log(log::Level::Info, "Performance sample started");
            scope.spawn(move || {
                let _ = ready.send(());
                if observe_started.recv().is_err() {
                    return 0;
                }
                let mut snapshots = 0;
                loop {
                    for session in hub.sessions() {
                        std::hint::black_box(session.snapshot());
                    }
                    std::hint::black_box(hub.logs());
                    snapshots += 1;
                    if snapshots == 1 {
                        let _ = ready.send(());
                    }
                    match stopping.recv_timeout(Duration::from_millis(200)) {
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return snapshots,
                    }
                }
            })
        });
        if observer.is_some() {
            started.recv().unwrap();
        }

        let timer = Instant::now();
        if observer.is_some() {
            start_observer.send(()).unwrap();
            // Первый кадр учитывается даже для передачи короче 200 ms;
            // создание потока по-прежнему не входит в сравнение.
            started.recv().unwrap();
        }
        begin.send(()).unwrap();
        let sent = send_dump_controlled(
            &mut session.control_stream,
            &mut session.data_stream,
            source.path().to_owned(),
            "observation-perf",
            BundlePlanOptions {
                bundle_target_bytes: 8 * 1024 * 1024,
                compression_level: 3,
                compression_threads: 1,
                readers: 4,
            },
            &sender,
        );
        if sent.is_err() {
            run.cancel();
        }
        let received = receive_worker.join().unwrap();
        let duration = timer.elapsed();
        drop(stop);
        if let Some(observer) = observer {
            assert!(observer.join().unwrap() > 0);
        }
        let (sent, profile) = sent.unwrap();
        let (received, _) = received.unwrap();
        assert_eq!(sent.terminal_skip_count, 0);
        assert_eq!(profile.bundled_entries, expected.len() as u64);
        if let Some(progress) = sender.progress() {
            assert_eq!(progress.snapshot().committed_entries, expected.len() as u64);
            assert!(progress.snapshot().scan_finished);
        }
        (duration, received)
    });
    let actual = read_archive_entries(&received.final_archive_path);
    // Сравнение сообщает только путь/размер: ошибочный большой файл не должен
    // печатать десятки MiB данных в вывод performance-прогона.
    assert_eq!(actual.len(), expected.len());
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(actual.path, expected.path);
        assert_eq!(actual.entry_type, expected.entry_type);
        assert_eq!(actual.link_name, expected.link_name);
        assert!(
            actual.bytes == expected.bytes,
            "payload mismatch: {}",
            actual.path
        );
    }
    duration
}

fn median(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[test]
#[ignore = "observational loopback performance comparison; run explicitly in release mode"]
fn telemetry_observation_overhead() {
    for (name, files, bytes_per_file) in [
        ("many-small", 2_000, 4 * 1024),
        ("streamed-large", 1, 64 * 1024 * 1024),
    ] {
        let (source, expected) = fixture(files, bytes_per_file);
        for observed in [false, true] {
            sample(&source, &expected, observed);
        }
        let mut baseline = Vec::with_capacity(REPETITIONS);
        let mut observed = Vec::with_capacity(REPETITIONS);
        for repetition in 0..REPETITIONS {
            for enabled in if repetition % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let duration = sample(&source, &expected, enabled);
                println!(
                    "PERF sample fixture={name} repetition={} observed={enabled} elapsed_ms={:.3}",
                    repetition + 1,
                    duration.as_secs_f64() * 1000.0
                );
                if enabled {
                    observed.push(duration);
                } else {
                    baseline.push(duration);
                }
            }
        }
        let baseline = median(&mut baseline).as_secs_f64();
        let observed = median(&mut observed).as_secs_f64();
        let mib = (files * bytes_per_file) as f64 / (1024.0 * 1024.0);
        println!(
            "PERF median fixture={name} repetitions={REPETITIONS} readers=4 threads=1 level=3 target_mib=8 observer_hz=5 baseline_ms={:.3} observed_ms={:.3} elapsed_delta_percent={:+.2} baseline_mib_s={:.2} observed_mib_s={:.2} archives_verified={}",
            baseline * 1000.0,
            observed * 1000.0,
            (observed / baseline - 1.0) * 100.0,
            mib / baseline,
            mib / observed,
            (REPETITIONS + 1) * 2
        );
    }
}
