use anyhow::{Result, ensure};
use ring::hmac;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::{ATTACH_CONTROL, ATTACH_DATA, AttachKey, MAGIC, random};

const CHALLENGE_LEN: usize = 16;
const TAG_LEN: usize = 32;

pub(super) struct Proof {
    message: [u8; 5 + CHALLENGE_LEN],
    tag: [u8; TAG_LEN],
}

impl Proof {
    pub(super) fn matches(&self, key: &AttachKey) -> bool {
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, key),
            &self.message,
            &self.tag,
        )
        .is_ok()
    }
}

pub(super) async fn receive(socket: &mut TcpStream) -> Result<(u8, Proof)> {
    let mut message = [0; 5 + CHALLENGE_LEN];
    let (header, challenge) = message.split_at_mut(5);
    socket.read_exact(header).await?;
    let kind = header[4];
    ensure!(
        &header[..4] == MAGIC && matches!(kind, ATTACH_CONTROL | ATTACH_DATA),
        "invalid relay socket attachment"
    );
    challenge.copy_from_slice(&random::<CHALLENGE_LEN>()?);
    socket.write_all(challenge).await?;
    let mut tag = [0; TAG_LEN];
    socket.read_exact(&mut tag).await?;
    Ok((kind, Proof { message, tag }))
}

pub(super) async fn send(socket: &mut TcpStream, kind: u8, key: &AttachKey) -> Result<()> {
    let mut message = [0; 5 + CHALLENGE_LEN];
    let (header, challenge) = message.split_at_mut(5);
    header[..4].copy_from_slice(MAGIC);
    header[4] = kind;
    socket.write_all(header).await?;
    socket.read_exact(challenge).await?;
    // Тикет остаётся в лобби, доказательство привязано к одному сокету и каналу.
    let tag = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), &message);
    socket.write_all(tag.as_ref()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attachment_proof_binds_the_ticket_challenge_and_channel() {
        let key = [0x42; 16];
        let mut message = [0xA5; 5 + CHALLENGE_LEN];
        message[..4].copy_from_slice(MAGIC);
        message[4] = ATTACH_CONTROL;
        let tag = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &key), &message);
        let mut proof = Proof {
            message,
            tag: tag.as_ref().try_into().unwrap(),
        };
        assert!(proof.matches(&key));
        assert!(!proof.matches(&[0x35; 16]));
        proof.message[4] = ATTACH_DATA;
        assert!(!proof.matches(&key));
        proof.message[4] = ATTACH_CONTROL;
        proof.message[5] ^= 1;
        assert!(!proof.matches(&key));
    }
}
