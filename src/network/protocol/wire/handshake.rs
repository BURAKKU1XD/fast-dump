use super::messages::{ChannelKind, ErrorMessage, Hello, HelloAck, Message, ResumeQuery};
use super::{VERSION, read_message, write_message};
use crate::network::network_io;
use crate::network::protocol::ResumeState;
use crate::role::Role;
use anyhow::{Context, Result, bail};
use std::io::{Read, Write};

pub(crate) fn request_resume_state<T>(stream: &mut T, dump_id: &str) -> Result<ResumeState>
where
    T: Read + Write,
{
    write_message(
        stream,
        &Message::ResumeQuery(ResumeQuery {
            dump_id: dump_id.to_owned(),
        }),
    )?;
    stream
        .flush()
        .map_err(network_io)
        .context("failed to flush v4 resume query to peer")?;

    match read_message(stream)? {
        Message::ResumeState(state) => Ok(state),
        Message::Error(error) => bail!("peer rejected v4 resume query: {}", error.message),
        unexpected => bail!(
            "unexpected v4 message while waiting for resume state: {:?}",
            unexpected.kind()
        ),
    }
}

pub(crate) fn initiate_handshake<T>(
    stream: &mut T,
    local_role: Role,
    dump_id: &str,
    token: Option<&str>,
    channel_kind: ChannelKind,
    session_nonce: u64,
) -> Result<Role>
where
    T: Read + Write,
{
    write_message(
        stream,
        &Message::Hello(Hello {
            role: local_role,
            protocol_version: VERSION,
            dump_id: dump_id.to_owned(),
            token: token.unwrap_or_default().to_owned(),
            channel_kind,
            session_nonce,
        }),
    )?;
    stream
        .flush()
        .map_err(network_io)
        .context("failed to flush v4 hello packet")?;

    match read_message(stream)? {
        Message::HelloAck(hello_ack) => {
            validate_peer_role(local_role, hello_ack.role)?;
            Ok(hello_ack.role)
        }
        Message::Error(error) => bail!("peer rejected v4 handshake: {}", error.message),
        other => bail!("unexpected v4 message during handshake: {:?}", other.kind()),
    }
}

pub(crate) fn respond_handshake_with_hello<T>(
    stream: &mut T,
    local_role: Role,
    expected_token: Option<&str>,
) -> Result<Hello>
where
    T: Read + Write,
{
    match read_message(stream)? {
        Message::Hello(hello) => {
            validate_hello_protocol_version(hello.protocol_version)
                .context("invalid v4 hello protocol version")?;

            if let Err(error) = validate_peer_role(local_role, hello.role) {
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            if let Err(error) = validate_hello_token(&hello.token, expected_token) {
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            if let Err(error) = crate::network::protocol::validate_dump_id(&hello.dump_id) {
                let _ = send_error(stream, &error.to_string());
                return Err(error);
            }

            write_message(stream, &Message::HelloAck(HelloAck { role: local_role }))?;
            stream
                .flush()
                .map_err(network_io)
                .context("failed to flush v4 hello ack packet")?;
            Ok(hello)
        }
        Message::Error(error) => {
            bail!("peer reported v4 error before handshake: {}", error.message)
        }
        other => {
            let _ = send_error(stream, "expected v4 hello packet");
            bail!("unexpected v4 message before handshake: {:?}", other.kind())
        }
    }
}

pub(crate) fn send_error<T>(stream: &mut T, message: &str) -> Result<()>
where
    T: Write,
{
    write_message(
        stream,
        &Message::Error(ErrorMessage {
            message: message.to_owned(),
        }),
    )?;
    stream
        .flush()
        .map_err(network_io)
        .context("failed to flush v4 error packet")
}

fn validate_peer_role(local_role: Role, remote_role: Role) -> Result<()> {
    if local_role == remote_role {
        bail!(
            "v4 handshake requires opposite roles, got local={} remote={}",
            local_role,
            remote_role
        );
    }
    Ok(())
}

fn validate_hello_protocol_version(protocol_version: u16) -> Result<()> {
    if protocol_version != VERSION {
        bail!(
            "v4 hello protocol version mismatch: expected {} got {}",
            VERSION,
            protocol_version
        );
    }
    Ok(())
}

fn validate_hello_token(token: &str, expected_token: Option<&str>) -> Result<()> {
    let Some(expected_token) = expected_token else {
        return Ok(());
    };
    if token != expected_token {
        bail!("bad token");
    }
    Ok(())
}
