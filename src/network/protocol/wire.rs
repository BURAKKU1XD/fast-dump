mod codec;
mod handshake;
mod messages;

use crate::network::network_io;
use anyhow::{Context, Result, bail};
use codec::{BodyReader, decode_message, encode_message};
pub(crate) use handshake::{
    initiate_handshake, request_resume_state, respond_handshake_with_hello, send_error,
};
pub(crate) use messages::*;
use std::io::{self, IoSlice, Read, Write};

pub(crate) const MAGIC: u32 = 0x3456_4446;
pub(crate) const VERSION: u16 = 4;
pub(crate) const MAX_DATA_CHUNK_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_CONTROL_BODY_BYTES: usize = 4 * 1024 * 1024;
const DATA_HEADER_BYTES: usize = 13;

#[derive(Debug)]
struct Packet {
    kind: MessageKind,
    body: Vec<u8>,
}

pub(crate) fn write_message<T>(stream: &mut T, message: &Message) -> Result<()>
where
    T: Write,
{
    if let Message::BundleCommit(commit) = message {
        // Фиксированный body не требует heap; до первого write сохраняем прежнюю валидацию.
        codec::validate_bundle_commit_descriptor(&commit.descriptor)?;
        let mut body = [0_u8; 36];
        body[..4].copy_from_slice(&commit.attempt_no.to_le_bytes());
        body[4..12].copy_from_slice(&commit.descriptor.bundle_no.to_le_bytes());
        body[12..20].copy_from_slice(&commit.descriptor.tar_payload_bytes.to_le_bytes());
        body[20..28].copy_from_slice(&commit.descriptor.compressed_payload_bytes.to_le_bytes());
        body[28..36].copy_from_slice(&commit.descriptor.compressed_xxh3_64.to_le_bytes());
        return write_packet(stream, MessageKind::BundleCommit, &body);
    }
    let (kind, body) = encode_message(message)?;
    write_packet(stream, kind, &body)
}

pub(crate) fn write_bundle_data_message<T>(
    stream: &mut T,
    bundle_no: u64,
    attempt_no: u32,
    is_last: bool,
    bytes: &[u8],
) -> Result<()>
where
    T: Write,
{
    if bytes.len() > MAX_DATA_CHUNK_BYTES {
        bail!(
            "v4 bundle data chunk is too large: {} > {} bytes",
            bytes.len(),
            MAX_DATA_CHUNK_BYTES
        );
    }

    let body_len = bytes
        .len()
        .checked_add(DATA_HEADER_BYTES)
        .context("v4 bundle data packet body is too large")?;
    let body_len = u32::try_from(body_len).context("v4 bundle data packet body is too large")?;
    let mut header = [0_u8; 12 + DATA_HEADER_BYTES];
    header[..12].copy_from_slice(&packet_header(MessageKind::BundleData, body_len));
    header[12..20].copy_from_slice(&bundle_no.to_le_bytes());
    header[20..24].copy_from_slice(&attempt_no.to_le_bytes());
    header[24] = u8::from(is_last);
    write_header_and_body(stream, &header, bytes)
        .map_err(network_io)
        .context("failed to write v4 bundle data packet")
}

pub(crate) fn read_message<T>(stream: &mut T) -> Result<Message>
where
    T: Read,
{
    let (kind, body_len) = read_packet_header(stream)?;
    if kind == MessageKind::BundleData {
        return read_bundle_data_body(stream, body_len).map(Message::BundleData);
    }
    read_control_body(stream, kind, body_len)
}

fn read_control_body<T: Read>(
    stream: &mut T,
    kind: MessageKind,
    body_len: usize,
) -> Result<Message> {
    let mut body = vec![0_u8; body_len];
    stream
        .read_exact(&mut body)
        .map_err(network_io)
        .context("failed to read v4 packet body")?;
    decode_message(Packet { kind, body })
}

fn read_bundle_data_body<T: Read>(stream: &mut T, body_len: usize) -> Result<BundleData> {
    let mut bytes = Vec::new();
    let header = read_bundle_data_body_into(stream, body_len, &mut bytes)?;
    Ok(BundleData {
        bundle_no: header.bundle_no,
        attempt_no: header.attempt_no,
        is_last: header.is_last,
        bytes,
    })
}

pub(crate) fn read_bundle_data_message_into<T: Read>(
    stream: &mut T,
    bytes: &mut Vec<u8>,
) -> Result<BundleDataHeader> {
    let (kind, body_len) = read_packet_header(stream)?;
    if kind != MessageKind::BundleData {
        match read_control_body(stream, kind, body_len)? {
            Message::Error(error) => {
                bail!("peer reported v4 data-channel error: {}", error.message)
            }
            other => bail!("unexpected v4 message on data channel: {:?}", other.kind()),
        };
    }
    read_bundle_data_body_into(stream, body_len, bytes)
}

fn read_bundle_data_body_into<T: Read>(
    stream: &mut T,
    body_len: usize,
    bytes: &mut Vec<u8>,
) -> Result<BundleDataHeader> {
    let payload_len = body_len
        .checked_sub(DATA_HEADER_BYTES)
        .context("v4 bundle data body is shorter than its header")?;
    let mut header = [0_u8; DATA_HEADER_BYTES];
    stream
        .read_exact(&mut header)
        .map_err(network_io)
        .context("failed to read v4 bundle data header")?;
    let mut reader = BodyReader::new(&header);
    let bundle_no = reader.read_u64()?;
    let attempt_no = reader.read_u32()?;
    let is_last = reader.read_bool("is_last")?;
    // Сохраняем инициализированный хвост между пакетами: повторное чтение не
    // требует ни выделения памяти, ни обнуления уже использованного буфера.
    if bytes.len() < payload_len {
        bytes.reserve_exact(payload_len - bytes.len());
        bytes.resize(payload_len, 0);
    }
    stream
        .read_exact(&mut bytes[..payload_len])
        .map_err(network_io)
        .context("failed to read v4 bundle data payload")?;
    Ok(BundleDataHeader {
        bundle_no,
        attempt_no,
        is_last,
        payload_len,
    })
}

fn packet_header(kind: MessageKind, body_len: u32) -> [u8; 12] {
    let mut header = [0_u8; 12];
    header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&(kind as u16).to_le_bytes());
    header[8..12].copy_from_slice(&body_len.to_le_bytes());
    header
}

fn write_packet<T>(stream: &mut T, kind: MessageKind, body: &[u8]) -> Result<()>
where
    T: Write,
{
    let body_len = u32::try_from(body.len()).context("v4 packet body too large")?;
    let max_body_len = kind.max_body_len();
    if body.len() > max_body_len {
        bail!(
            "v4 packet body is too large for {:?}: {} > {} bytes",
            kind,
            body.len(),
            max_body_len
        );
    }

    let header = packet_header(kind, body_len);
    write_header_and_body(stream, &header, body)
        .map_err(network_io)
        .context("failed to write v4 packet")
}

fn write_header_and_body<T: Write>(stream: &mut T, header: &[u8], body: &[u8]) -> io::Result<()> {
    // TCP отправляет заголовок и payload одним syscall без объединяющего буфера.
    // Для обычного Write стандартный fallback продвигает первый непустой срез.
    let mut slices = [IoSlice::new(header), IoSlice::new(body)];
    let mut remaining = &mut slices[..];
    while !remaining.is_empty() {
        match stream.write_vectored(remaining) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => IoSlice::advance_slices(&mut remaining, written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn read_packet_header<T>(stream: &mut T) -> Result<(MessageKind, usize)>
where
    T: Read,
{
    let mut header = [0_u8; 12];
    stream
        .read_exact(&mut header)
        .map_err(network_io)
        .context("failed to read v4 packet header")?;

    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != MAGIC {
        bail!("invalid v4 packet magic: {magic:#010x}");
    }

    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != VERSION {
        bail!("unsupported v4 protocol version: {version}");
    }

    let kind = MessageKind::from_u16(u16::from_le_bytes(header[6..8].try_into().unwrap()))?;
    let body_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let max_body_len = kind.max_body_len();
    if body_len > max_body_len {
        bail!(
            "v4 packet body is too large for {:?}: {} > {} bytes",
            kind,
            body_len,
            max_body_len
        );
    }

    Ok((kind, body_len))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::codec::BodyWriter;
    use super::*;
    use crate::role::Role;
    use std::io::Cursor;

    #[test]
    fn malformed_complete_body_is_not_a_retryable_network_eof() {
        let mut packet = Vec::new();
        write_packet(&mut packet, MessageKind::BundleAck, &[]).unwrap();
        let error = read_message(&mut Cursor::new(packet)).unwrap_err();
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert!(!crate::network::is_retryable_error(&error));
    }

    #[test]
    fn socket_eof_and_reset_are_retryable_network_failures() {
        use std::net::{Shutdown, TcpListener, TcpStream};
        use std::time::Duration;

        for reset in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            if reset {
                socket2::SockRef::from(&peer)
                    .set_linger(Some(Duration::ZERO))
                    .unwrap();
                drop(peer);
            } else {
                peer.shutdown(Shutdown::Write).unwrap();
            }
            let error = read_message(&mut stream).unwrap_err();
            assert!(crate::network::is_retryable_error(&error), "{error:#}");
            assert!(matches!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
            ));
        }
    }

    #[test]
    fn control_strings_reject_inner_lengths_beyond_received_body() {
        for length in [1, u32::MAX] {
            let body = length.to_le_bytes();
            for kind in [MessageKind::ResumeQuery, MessageKind::Error] {
                let mut packet = Vec::new();
                write_packet(&mut packet, kind, &body).unwrap();
                let error = read_message(&mut Cursor::new(packet)).unwrap_err();
                assert!(format!("{error:#}").contains("string length"));
            }
        }
        let mut writer = BodyWriter::new();
        writer.write_string("архив 🙂").unwrap();
        let body = writer.finish();
        let mut reader = BodyReader::new(&body);
        assert_eq!(reader.read_string().unwrap(), "архив 🙂");
        reader.finish().unwrap();
    }

    #[test]
    fn control_entry_counts_are_bounded_before_reserving_memory() {
        for count in [1, u32::MAX] {
            for kind in [
                MessageKind::SkipEntries,
                MessageKind::ResumeState,
                MessageKind::BundlePlan,
            ] {
                let mut body = BodyWriter::new();
                match kind {
                    MessageKind::ResumeState => {
                        body.write_u64(0);
                        body.write_u64(0);
                        body.write_string("").unwrap();
                        body.write_u64(0);
                    }
                    MessageKind::BundlePlan => {
                        body.write_u32(1);
                        body.write_u64(1);
                        body.write_u64(1);
                        body.write_u64(1);
                        body.write_string("file").unwrap();
                        body.write_u64(0);
                    }
                    _ => {}
                }
                body.write_u32(count);
                let mut packet = Vec::new();
                write_packet(&mut packet, kind, &body.finish()).unwrap();
                let error = read_message(&mut Cursor::new(packet)).unwrap_err();
                assert!(format!("{error:#}").contains("entry count exceeds"));
            }
        }
    }

    #[test]
    fn handshake_rejects_unsafe_dump_id_before_acknowledging() {
        struct Duplex {
            input: Cursor<Vec<u8>>,
            output: Vec<u8>,
        }
        impl Read for Duplex {
            fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
                self.input.read(bytes)
            }
        }
        impl Write for Duplex {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.output.write(bytes)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        for dump_id in ["../outside", "dump:stream", "CON", ""] {
            let mut input = Vec::new();
            write_message(
                &mut input,
                &Message::Hello(Hello {
                    role: Role::Sender,
                    protocol_version: VERSION,
                    dump_id: dump_id.to_owned(),
                    token: String::new(),
                    channel_kind: ChannelKind::Control,
                    session_nonce: 1,
                }),
            )
            .unwrap();
            let mut duplex = Duplex {
                input: Cursor::new(input),
                output: Vec::new(),
            };
            assert!(respond_handshake_with_hello(&mut duplex, Role::Receiver, None).is_err());
            assert!(matches!(
                read_message(&mut Cursor::new(duplex.output)).unwrap(),
                Message::Error(_)
            ));
        }
    }

    #[derive(Clone, Copy)]
    enum WriteStep {
        Limit(usize),
        Fail(io::ErrorKind),
    }

    #[derive(Default)]
    struct ScriptedVectoredWriter {
        steps: VecDeque<WriteStep>,
        bytes: Vec<u8>,
        calls: usize,
    }

    impl Write for ScriptedVectoredWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            panic!("packet bypassed write_vectored");
        }

        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            self.calls += 1;
            let mut remaining = match self.steps.pop_front() {
                Some(WriteStep::Limit(limit)) => limit,
                Some(WriteStep::Fail(kind)) => return Err(kind.into()),
                None => usize::MAX,
            };
            let mut written = 0;
            for buf in bufs {
                let len = remaining.min(buf.len());
                self.bytes.extend_from_slice(&buf[..len]);
                remaining -= len;
                written += len;
            }
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn reference_packet(message: &Message) -> Vec<u8> {
        let (kind, body) = encode_message(message).unwrap();
        let mut bytes = packet_header(kind, body.len() as u32).to_vec();
        bytes.extend_from_slice(&body);
        bytes
    }

    fn commit_message(attempt_no: u32, fields: [u64; 4]) -> Message {
        Message::BundleCommit(BundleCommit {
            attempt_no,
            descriptor: crate::network::protocol::BundleCommitDescriptor {
                bundle_no: fields[0],
                tar_payload_bytes: fields[1],
                compressed_payload_bytes: fields[2],
                compressed_xxh3_64: fields[3],
            },
        })
    }

    #[test]
    fn stack_commit_matches_generic_encoder_for_edge_values() {
        for attempt in [0, 1, u32::MAX] {
            for fields in [
                [1, 0, 1, 0],
                [17, 0x0102_0304_0506_0708, 9, 0xf1e2_d3c4_b5a6_9788],
                [u64::MAX; 4],
            ] {
                let message = commit_message(attempt, fields);
                let mut packet = Vec::new();
                write_message(&mut packet, &message).unwrap();
                assert_eq!(packet, reference_packet(&message));
                assert_eq!(packet.len(), 12 + 36);
                assert_eq!(read_message(&mut Cursor::new(packet)).unwrap(), message);
            }
        }
    }

    #[test]
    fn stack_commit_rejects_invalid_descriptor_before_any_write() {
        for fields in [[0, 0, 1, 0], [1, 0, 0, 0], [0, 0, 0, 0]] {
            let message = commit_message(u32::MAX, fields);
            let expected = encode_message(&message).unwrap_err().to_string();
            let mut writer = ScriptedVectoredWriter::default();
            let error = write_message(&mut writer, &message).unwrap_err();
            assert_eq!(error.to_string(), expected);
            assert!(writer.bytes.is_empty());
            assert_eq!(writer.calls, 0);
        }
    }

    #[test]
    fn stack_commit_preserves_partial_and_interrupted_writes() {
        let message = commit_message(3, [17, 1024, 999, u64::MAX]);
        for limit in [1, 11, 12, 13, 47] {
            let mut writer = ScriptedVectoredWriter {
                steps: VecDeque::from([
                    WriteStep::Fail(io::ErrorKind::Interrupted),
                    WriteStep::Limit(limit),
                    WriteStep::Fail(io::ErrorKind::Interrupted),
                    WriteStep::Limit(limit),
                ]),
                ..Default::default()
            };
            write_message(&mut writer, &message).unwrap();
            assert_eq!(writer.bytes, reference_packet(&message));
        }
        for kind in [io::ErrorKind::WriteZero, io::ErrorKind::BrokenPipe] {
            let mut writer = ScriptedVectoredWriter {
                steps: VecDeque::from([
                    WriteStep::Limit(5),
                    if kind == io::ErrorKind::WriteZero {
                        WriteStep::Limit(0)
                    } else {
                        WriteStep::Fail(kind)
                    },
                ]),
                ..Default::default()
            };
            let error = write_message(&mut writer, &message).unwrap_err();
            assert_eq!(error.downcast_ref::<io::Error>().unwrap().kind(), kind);
            assert!(crate::network::is_retryable_error(&error));
            assert_eq!(writer.bytes, reference_packet(&message)[..5]);
            assert_eq!(writer.calls, 2);
        }
    }

    #[test]
    fn vectored_writes_preserve_partial_progress_across_header_and_payload() {
        let control = Message::ResumeQuery(ResumeQuery {
            dump_id: "all-files".into(),
        });
        let data = BundleData {
            bundle_no: 17,
            attempt_no: 3,
            is_last: true,
            bytes: vec![0xa5; 41],
        };
        for limit in [1, 11, 12, 13, 24, 25, 26, 29] {
            let steps = VecDeque::from([
                WriteStep::Fail(io::ErrorKind::Interrupted),
                WriteStep::Limit(limit),
                WriteStep::Fail(io::ErrorKind::Interrupted),
                WriteStep::Limit(limit),
            ]);
            let mut writer = ScriptedVectoredWriter {
                steps: steps.clone(),
                ..Default::default()
            };
            write_message(&mut writer, &control).unwrap();
            assert_eq!(writer.bytes, reference_packet(&control));

            let mut writer = ScriptedVectoredWriter {
                steps,
                ..Default::default()
            };
            write_bundle_data_message(
                &mut writer,
                data.bundle_no,
                data.attempt_no,
                data.is_last,
                &data.bytes,
            )
            .unwrap();
            assert_eq!(
                writer.bytes,
                reference_packet(&Message::BundleData(data.clone()))
            );
        }
    }

    #[test]
    fn vectored_writes_surface_write_zero_and_nonretryable_errors() {
        for step in [
            WriteStep::Limit(0),
            WriteStep::Fail(io::ErrorKind::BrokenPipe),
        ] {
            let expected_kind = match step {
                WriteStep::Limit(_) => io::ErrorKind::WriteZero,
                WriteStep::Fail(kind) => kind,
            };
            let mut writer = ScriptedVectoredWriter {
                steps: VecDeque::from([WriteStep::Limit(5), step]),
                ..Default::default()
            };
            let error = write_bundle_data_message(&mut writer, 1, 1, false, &[42; 32]).unwrap_err();
            assert_eq!(
                error.downcast_ref::<io::Error>().unwrap().kind(),
                expected_kind
            );
            assert_eq!(writer.bytes.len(), 5);
            assert_eq!(writer.calls, 2);
        }
    }

    #[test]
    fn vectored_transport_accepts_header_and_payload_in_one_call() {
        let control = Message::ResumeQuery(ResumeQuery {
            dump_id: "all-files".into(),
        });
        for message in [&control, &Message::FinishAck] {
            let mut writer = ScriptedVectoredWriter::default();
            write_message(&mut writer, message).unwrap();
            assert_eq!(writer.bytes, reference_packet(message));
            assert_eq!(writer.calls, 1);
        }
        for payload in [&[][..], &[42; 32][..]] {
            let mut writer = ScriptedVectoredWriter::default();
            write_bundle_data_message(&mut writer, 1, 1, true, payload).unwrap();
            assert_eq!(writer.calls, 1);
            assert_eq!(
                writer.bytes,
                reference_packet(&Message::BundleData(BundleData {
                    bundle_no: 1,
                    attempt_no: 1,
                    is_last: true,
                    bytes: payload.to_vec(),
                }))
            );
        }
    }

    #[test]
    fn scalar_write_fallback_preserves_complete_packet_bytes() {
        struct ScalarWriter(Vec<u8>);
        impl Write for ScalarWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                let len = buf.len().min(7);
                self.0.extend_from_slice(&buf[..len]);
                Ok(len)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut writer = ScalarWriter(Vec::new());
        write_bundle_data_message(&mut writer, 2, 3, true, &[0xab; 33]).unwrap();
        assert_eq!(
            writer.0,
            reference_packet(&Message::BundleData(BundleData {
                bundle_no: 2,
                attempt_no: 3,
                is_last: true,
                bytes: vec![0xab; 33],
            }))
        );
    }

    #[test]
    fn direct_bundle_data_codec_preserves_wire_bytes_at_payload_boundaries() {
        for size in [0, 1, 4097, MAX_DATA_CHUNK_BYTES] {
            for is_last in [false, true] {
                let data = BundleData {
                    bundle_no: u64::MAX - 1,
                    attempt_no: u32::MAX,
                    is_last,
                    bytes: (0..size).map(|n| n as u8).collect(),
                };
                let expected = Message::BundleData(data.clone());
                let mut generic = Vec::new();
                write_message(&mut generic, &expected).unwrap();
                let mut direct = Vec::new();
                write_bundle_data_message(
                    &mut direct,
                    data.bundle_no,
                    data.attempt_no,
                    data.is_last,
                    &data.bytes,
                )
                .unwrap();
                assert_eq!(direct, generic);
                assert_eq!(read_message(&mut Cursor::new(direct)).unwrap(), expected);
            }
        }
    }

    struct FragmentedReader(Cursor<Vec<u8>>);

    impl Read for FragmentedReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let len = buf.len().min(7);
            self.0.read(&mut buf[..len])
        }
    }

    #[test]
    fn direct_bundle_data_read_handles_fragmentation_and_next_packet() {
        let data = BundleData {
            bundle_no: 3,
            attempt_no: 2,
            is_last: true,
            bytes: vec![0xab; 1025],
        };
        let mut bytes = Vec::new();
        write_message(&mut bytes, &Message::BundleData(data.clone())).unwrap();
        write_message(&mut bytes, &Message::FinishAck).unwrap();
        let mut input = FragmentedReader(Cursor::new(bytes));
        assert_eq!(read_message(&mut input).unwrap(), Message::BundleData(data));
        assert_eq!(read_message(&mut input).unwrap(), Message::FinishAck);
    }

    #[test]
    fn reused_data_buffer_keeps_initialized_tail_and_packet_boundaries() {
        let mut packets = Vec::new();
        for (index, len) in [1025, 7, 0, 1025].into_iter().enumerate() {
            write_bundle_data_message(
                &mut packets,
                index as u64,
                2,
                index == 3,
                &vec![index as u8; len],
            )
            .unwrap();
        }
        write_message(&mut packets, &Message::FinishAck).unwrap();
        let mut input = FragmentedReader(Cursor::new(packets));
        let mut storage = Vec::new();
        let first = read_bundle_data_message_into(&mut input, &mut storage).unwrap();
        assert_eq!(first.payload_len, 1025);
        let allocation = storage.as_ptr();
        storage[7..].fill(0xa5);
        for (index, len) in [(1, 7), (2, 0), (3, 1025)] {
            let header = read_bundle_data_message_into(&mut input, &mut storage).unwrap();
            assert_eq!(header.bundle_no, index);
            assert_eq!(header.attempt_no, 2);
            assert_eq!(header.is_last, index == 3);
            assert_eq!(header.payload_len, len);
            assert_eq!(storage.len(), 1025);
            assert_eq!(storage.as_ptr(), allocation);
            assert!(storage[..len].iter().all(|byte| *byte == index as u8));
            if index < 3 {
                assert!(storage[7..].iter().all(|byte| *byte == 0xa5));
            }
        }
        assert_eq!(read_message(&mut input).unwrap(), Message::FinishAck);
    }

    #[test]
    fn reusable_data_reader_rejects_truncation_and_wrong_channel_messages() {
        let mut packet = Vec::new();
        write_bundle_data_message(&mut packet, 1, 1, true, &[0xa5; 32]).unwrap();
        for len in 0..packet.len() {
            assert!(
                read_bundle_data_message_into(&mut Cursor::new(&packet[..len]), &mut Vec::new())
                    .is_err()
            );
        }
        let mut error = Vec::new();
        send_error(&mut error, "cancelled").unwrap();
        assert!(
            read_bundle_data_message_into(&mut Cursor::new(error), &mut Vec::new())
                .unwrap_err()
                .to_string()
                .contains("data-channel error: cancelled")
        );
        let mut wrong = Vec::new();
        write_message(&mut wrong, &Message::FinishAck).unwrap();
        assert!(
            read_bundle_data_message_into(&mut Cursor::new(wrong), &mut Vec::new())
                .unwrap_err()
                .to_string()
                .contains("unexpected v4 message on data channel")
        );
    }

    #[test]
    fn direct_bundle_data_read_rejects_every_truncated_prefix() {
        let mut bytes = Vec::new();
        write_bundle_data_message(&mut bytes, 1, 1, true, &[0xa5; 32]).unwrap();
        for len in 0..bytes.len() {
            assert!(
                read_message(&mut Cursor::new(&bytes[..len])).is_err(),
                "length {len}"
            );
        }
    }

    #[test]
    fn direct_bundle_data_read_rejects_short_metadata_and_invalid_boolean() {
        for body_len in 0..DATA_HEADER_BYTES {
            let mut bytes = packet_header(MessageKind::BundleData, body_len as u32).to_vec();
            bytes.resize(12 + body_len, 0);
            assert!(read_message(&mut Cursor::new(bytes)).is_err());
        }
        for is_last in [2, 255] {
            let mut bytes = Vec::new();
            write_bundle_data_message(&mut bytes, 1, 1, false, &[]).unwrap();
            bytes[24] = is_last;
            assert!(read_message(&mut Cursor::new(bytes)).is_err());
        }
    }

    #[test]
    fn direct_bundle_data_read_retains_packet_header_validation() {
        let header = packet_header(MessageKind::BundleData, DATA_HEADER_BYTES as u32);
        for offset in [0, 4, 6] {
            let mut bytes = header;
            bytes[offset] = 0xff;
            assert!(read_message(&mut Cursor::new(bytes)).is_err());
        }
        let oversized = packet_header(
            MessageKind::BundleData,
            (MAX_DATA_CHUNK_BYTES + DATA_HEADER_BYTES + 1) as u32,
        );
        let error = read_message(&mut Cursor::new(oversized)).unwrap_err();
        assert!(error.to_string().contains("too large"));
    }
}
