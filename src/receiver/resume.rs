use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use log::debug;

use crate::network::protocol::{self, Message};

use super::ReceiverState;

#[cfg_attr(not(test), allow(dead_code))]
pub fn respond_resume_query<T>(
    stream: &mut T,
    data_dir: impl Into<PathBuf>,
) -> Result<ReceiverState>
where
    T: Read + Write,
{
    respond_resume_query_checked(stream, data_dir, None)
}

// В normal runtime receiver уже знает dump_id из Hello и может
// отсеять ResumeQuery, который пытается переключиться на другой dump.
pub fn respond_resume_query_checked<T>(
    stream: &mut T,
    data_dir: impl Into<PathBuf>,
    expected_dump_id: Option<&str>,
) -> Result<ReceiverState>
where
    T: Read + Write,
{
    let query = match protocol::read_message(stream)? {
        Message::ResumeQuery(query) => query,
        Message::Error(error) => {
            bail!("peer aborted before v3 resume query: {}", error.message);
        }
        unexpected => {
            protocol::write_message(
                stream,
                &Message::Error(protocol::ErrorMessage {
                    message: "expected v3 resume query".to_owned(),
                }),
            )?;
            stream
                .flush()
                .context("failed to flush error response for invalid v3 resume query")?;
            bail!(
                "unexpected v3 message before resume query: {:?}",
                unexpected.kind()
            );
        }
    };

    if let Err(error) = validate_resume_query_dump_id(&query.dump_id, expected_dump_id) {
        let message = error.to_string();
        protocol::write_message(stream, &Message::Error(protocol::ErrorMessage { message }))?;
        stream
            .flush()
            .context("failed to flush error response for invalid v3 resume query")?;
        return Err(error);
    }
    debug!(
        "receiver v3 resume query accepted: dump_id={} expected_dump_id={:?}",
        query.dump_id, expected_dump_id
    );

    let state = match ReceiverState::open(data_dir, query.dump_id) {
        Ok(state) => state,
        Err(error) => {
            let message = error.to_string();
            protocol::write_message(
                stream,
                &Message::Error(protocol::ErrorMessage {
                    message: message.clone(),
                }),
            )?;
            stream
                .flush()
                .context("failed to flush receiver error for v3 resume query")?;
            return Err(error.context("failed to open receiver state for v3 resume query"));
        }
    };

    protocol::write_message(stream, &Message::ResumeState(state.resume_state()))?;
    stream
        .flush()
        .context("failed to flush v3 resume state to peer")?;
    debug!(
        "receiver v3 resume state sent: dump_id={} frame={} work_ordinal={:?} archive_len={}",
        state.paths.dump_id(),
        state.journal.last_committed_frame,
        state.journal.last_committed_work_ordinal,
        state.journal.archive_len
    );

    Ok(state)
}

fn validate_resume_query_dump_id(
    query_dump_id: &str,
    expected_dump_id: Option<&str>,
) -> Result<()> {
    let Some(expected_dump_id) = expected_dump_id else {
        return Ok(());
    };

    if query_dump_id != expected_dump_id {
        bail!(
            "resume dump_id mismatch: hello announced {} but resume query requested {}",
            expected_dump_id,
            query_dump_id
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, path::PathBuf};

    use super::*;
    use crate::network::protocol::{ErrorMessage, Message, ResumeQuery, read_message};
    use crate::receiver::test_support::TestDir;

    fn encode_message(message: &Message) -> Vec<u8> {
        let mut bytes = Vec::new();
        protocol::write_message(&mut bytes, message).unwrap();
        bytes
    }

    fn decode_reply(bytes: &[u8], request_len: usize) -> Message {
        read_message(&mut Cursor::new(bytes[request_len..].to_vec())).unwrap()
    }

    #[test]
    fn respond_resume_query_replies_with_resume_state_on_success() {
        let temp = TestDir::new();
        let request = encode_message(&Message::ResumeQuery(ResumeQuery {
            dump_id: "dump-v3".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let state = respond_resume_query(&mut stream, temp.path().to_path_buf()).unwrap();
        let bytes = stream.into_inner();

        assert_eq!(state.paths.dump_id(), "dump-v3");
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::ResumeState(state.resume_state())
        );
    }

    #[test]
    fn respond_resume_query_rejects_dump_id_mismatch_with_hello_and_emits_error() {
        let temp = TestDir::new();
        let request = encode_message(&Message::ResumeQuery(ResumeQuery {
            dump_id: "dump-02".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error =
            respond_resume_query_checked(&mut stream, temp.path().to_path_buf(), Some("dump-01"))
                .unwrap_err();
        let bytes = stream.into_inner();

        assert!(error.to_string().contains(
            "resume dump_id mismatch: hello announced dump-01 but resume query requested dump-02"
        ));
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message:
                    "resume dump_id mismatch: hello announced dump-01 but resume query requested dump-02"
                        .to_owned(),
            })
        );
    }

    #[test]
    fn respond_resume_query_rejects_unexpected_message_and_emits_error() {
        let request = encode_message(&Message::FinishAck);
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_resume_query(&mut stream, PathBuf::from("state")).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("unexpected v3 message before resume query")
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "expected v3 resume query".to_owned(),
            })
        );
    }

    #[test]
    fn respond_resume_query_surfaces_peer_error_without_reply() {
        let request = encode_message(&Message::Error(ErrorMessage {
            message: "cancelled".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_resume_query(&mut stream, PathBuf::from("state")).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("peer aborted before v3 resume query: cancelled")
        );
        assert_eq!(bytes.len(), request_len);
    }

    #[test]
    fn respond_resume_query_reports_receiver_state_open_error_to_peer() {
        let temp = TestDir::new();
        let request = encode_message(&Message::ResumeQuery(ResumeQuery {
            dump_id: "nested/dump".to_owned(),
        }));
        let request_len = request.len();
        let mut stream = Cursor::new(request);

        let error = respond_resume_query(&mut stream, temp.path().to_path_buf()).unwrap_err();
        let bytes = stream.into_inner();

        assert!(
            error
                .to_string()
                .contains("failed to open receiver state for v3 resume query")
        );
        assert_eq!(
            decode_reply(&bytes, request_len),
            Message::Error(ErrorMessage {
                message: "dump_id must be a single safe path component".to_owned(),
            })
        );
    }
}
