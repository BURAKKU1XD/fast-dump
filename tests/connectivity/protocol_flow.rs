use std::fs;
use std::thread;

use crate::support::TestDir;
use fast_dump::network::protocol::{Ack, ResumeState, request_resume_state};
use fast_dump::network::transport::{BoundListener, connect_once};
use fast_dump::receiver::{receive_finish, receive_frame, respond_resume_query};
use fast_dump::role::Role;
use fast_dump::sender::{OutboundFrame, prepare_dump, send_finish, send_frame};

#[test]
fn frame_protocol_commits_single_and_multi_frame_sequences() {
    {
        let label = "single-frame";
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let data_dir = TestDir::new();
        let receiver_dir = data_dir.path().to_path_buf();
        let frame = OutboundFrame {
            frame_no: 1,
            first_file_index: 1,
            first_file_path: "root/a.bin".to_owned(),
            last_file_index: 2,
            last_file_path: "root/b.bin".to_owned(),
            level: 3,
            threads: 2,
        };

        let receiver = thread::spawn(move || {
            let mut session = listener.accept(Role::Receiver, None, None).unwrap();
            let mut state = respond_resume_query(&mut session.stream, receiver_dir).unwrap();
            let ack = receive_frame(&mut session.stream, &mut state).unwrap();
            (
                ack,
                state.journal.archive_len,
                state.journal.last_committed_frame,
            )
        });

        let mut sender_session = connect_once(Role::Sender, addr, Some("dump-05"), None).unwrap();
        let resume_state = request_resume_state(&mut sender_session.stream, "dump-05").unwrap();
        assert_eq!(resume_state.archive_len, 0, "{label}");

        let ack = send_frame(
            &mut sender_session.stream,
            &frame,
            [b"abc".as_slice(), b"def".as_slice()],
        )
        .unwrap();
        let (receiver_ack, archive_len, committed_frame) = receiver.join().unwrap();

        assert_eq!(
            ack,
            Ack {
                frame_no: 1,
                last_file_index: 2,
                last_file_path: "root/b.bin".to_owned(),
                archive_len: 6,
            },
            "{label}"
        );
        assert_eq!(receiver_ack, ack, "{label}");
        assert_eq!(archive_len, 6, "{label}");
        assert_eq!(committed_frame, 1, "{label}");
    }

    {
        let label = "multi-frame";
        let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        let data_dir = TestDir::new();
        let receiver_dir = data_dir.path().to_path_buf();
        let frame1 = OutboundFrame {
            frame_no: 1,
            first_file_index: 1,
            first_file_path: "root/a.bin".to_owned(),
            last_file_index: 1,
            last_file_path: "root/a.bin".to_owned(),
            level: 3,
            threads: 2,
        };
        let frame2 = OutboundFrame {
            frame_no: 2,
            first_file_index: 2,
            first_file_path: "root/b.bin".to_owned(),
            last_file_index: 3,
            last_file_path: "root/c.bin".to_owned(),
            level: 3,
            threads: 2,
        };

        let receiver = thread::spawn(move || {
            let mut session = listener.accept(Role::Receiver, None, None).unwrap();
            let mut state = respond_resume_query(&mut session.stream, receiver_dir).unwrap();
            let ack1 = receive_frame(&mut session.stream, &mut state).unwrap();
            let ack2 = receive_frame(&mut session.stream, &mut state).unwrap();
            let finish = receive_finish(&mut session.stream, &state).unwrap();
            (ack1, ack2, finish, state.journal.archive_len)
        });

        let mut sender_session = connect_once(Role::Sender, addr, Some("dump-06"), None).unwrap();
        let resume_state = request_resume_state(&mut sender_session.stream, "dump-06").unwrap();
        assert_eq!(resume_state.archive_len, 0, "{label}");

        let ack1 = send_frame(&mut sender_session.stream, &frame1, [b"aaa".as_slice()]).unwrap();
        let ack2 = send_frame(
            &mut sender_session.stream,
            &frame2,
            [b"bbbb".as_slice(), b"cc".as_slice()],
        )
        .unwrap();
        let finish = send_finish(
            &mut sender_session.stream,
            frame2.frame_no,
            frame2.last_file_index,
            &frame2.last_file_path,
        )
        .unwrap();

        let (receiver_ack1, receiver_ack2, receiver_finish, archive_len) = receiver.join().unwrap();

        assert_eq!(ack1.archive_len, 3, "{label}");
        assert_eq!(ack2.archive_len, 9, "{label}");
        assert_eq!(receiver_ack1, ack1, "{label}");
        assert_eq!(receiver_ack2, ack2, "{label}");
        assert_eq!(finish.final_frame_no, 2, "{label}");
        assert_eq!(finish.final_file_index, 3, "{label}");
        assert_eq!(finish.final_file_path, "root/c.bin", "{label}");
        assert_eq!(receiver_finish, finish, "{label}");
        assert_eq!(archive_len, 9, "{label}");
    }
}

#[test]
fn sender_pipeline_roundtrips_into_valid_tar_zst_archive() {
    let source_dir = TestDir::new();
    fs::create_dir_all(source_dir.path().join("nested")).unwrap();
    fs::write(source_dir.path().join("alpha.txt"), b"alpha").unwrap();
    fs::write(source_dir.path().join("nested").join("beta.txt"), b"beta").unwrap();

    let prepared = prepare_dump(
        source_dir.path().to_path_buf(),
        &ResumeState {
            last_committed_frame: 0,
            last_committed_file_index: 0,
            last_committed_file_path: String::new(),
            archive_len: 0,
        },
        3,
        1,
        1,
    )
    .unwrap();

    let listener = BoundListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr();
    let receiver_dir = TestDir::new();
    let receiver_path = receiver_dir.path().to_path_buf();
    let expected_frame_count = prepared.frames.len();

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        let mut state = respond_resume_query(&mut session.stream, receiver_path).unwrap();
        for _ in 0..expected_frame_count {
            receive_frame(&mut session.stream, &mut state).unwrap();
        }
        let finish = receive_finish(&mut session.stream, &state).unwrap();
        (finish, state.paths.final_archive_path().to_path_buf())
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some("dump-07"), None).unwrap();
    let resume_state = request_resume_state(&mut sender_session.stream, "dump-06").unwrap();
    assert_eq!(resume_state.archive_len, 0);

    for frame in &prepared.frames {
        let chunk_refs: Vec<&[u8]> = frame.chunks.iter().map(Vec::as_slice).collect();
        send_frame(&mut sender_session.stream, &frame.outbound, chunk_refs).unwrap();
    }
    let finish = send_finish(
        &mut sender_session.stream,
        prepared.final_frame_no,
        prepared.final_file_index,
        &prepared.final_file_path,
    )
    .unwrap();

    let (receiver_finish, archive_path) = receiver.join().unwrap();
    assert_eq!(receiver_finish, finish);
    assert!(archive_path.exists());
    assert!(!archive_path.with_extension("zst.part").exists());

    let file = fs::File::open(&archive_path).unwrap();
    let decoder = zstd::stream::read::Decoder::new(file).unwrap();
    let mut archive = tar::Archive::new(decoder);
    let mut entries = Vec::new();
    for entry in archive.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().to_string_lossy().into_owned();
        let mut bytes = Vec::new();
        std::io::copy(&mut entry, &mut bytes).unwrap();
        entries.push((path, bytes));
    }

    assert_eq!(
        entries,
        vec![
            ("alpha.txt".to_owned(), b"alpha".to_vec()),
            ("nested/beta.txt".to_owned(), b"beta".to_vec()),
        ]
    );
}
