use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::rustls::pki_types::ServerName;

use super::super::ClientCommands;

const RELEASE_ROOT: &str = "https://github.com/BURAKKU1XD/fast-dump/releases/latest/download";
const IP_HOST: &str = "api.ipify.org";
const IP_LOOKUP_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_IP_RESPONSE: u64 = 4096;
const MAX_IP_BODY: usize = 32;

pub(super) async fn client_commands(listen: SocketAddr, token: &str) -> ClientCommands {
    let public_ip = if listen.ip() == Ipv4Addr::UNSPECIFIED {
        match timeout(IP_LOOKUP_TIMEOUT, discover_ipv4()).await {
            Ok(Ok(ip)) => Some(ip),
            Ok(Err(error)) => {
                log::debug!("Relay public IPv4 lookup failed: {error:#}");
                None
            }
            Err(_) => {
                log::debug!("Relay public IPv4 lookup timed out");
                None
            }
        }
    } else {
        None
    };
    commands(listen, public_ip, token)
}

async fn discover_ipv4() -> Result<Ipv4Addr> {
    let tls = super::super::client::connector(None)?;
    let socket = TcpStream::connect((IP_HOST, 443)).await?;
    let mut stream = tls.connect(ServerName::try_from(IP_HOST)?, socket).await?;
    // HTTP/1.0 запрашивает ответ без chunked-кодирования.
    let request = format!("GET / HTTP/1.0\r\nHost: {IP_HOST}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    read_ipv4_response(stream).await
}

async fn read_ipv4_response(stream: impl AsyncRead + Unpin) -> Result<Ipv4Addr> {
    let mut input = BufReader::new(stream.take(MAX_IP_RESPONSE));
    let mut line = String::new();
    input.read_line(&mut line).await?;
    let mut status = line.split_ascii_whitespace();
    ensure!(
        matches!(status.next(), Some("HTTP/1.0" | "HTTP/1.1")) && status.next() == Some("200"),
        "unexpected public IP service status"
    );

    let mut content_length = None;
    loop {
        line.clear();
        ensure!(
            input.read_line(&mut line).await? > 0,
            "incomplete public IP response headers"
        );
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            ensure!(
                !name.eq_ignore_ascii_case("Transfer-Encoding"),
                "unexpected transfer encoding in public IP response"
            );
            if name.eq_ignore_ascii_case("Content-Length") {
                ensure!(
                    content_length.is_none(),
                    "duplicate public IP response length"
                );
                content_length = Some(value.trim().parse::<usize>()?);
            }
        }
    }

    let mut body = Vec::new();
    if let Some(length) = content_length {
        ensure!(
            (1..=MAX_IP_BODY).contains(&length),
            "invalid public IP response length"
        );
        body.resize(length, 0);
        input.read_exact(&mut body).await?;
    } else {
        (&mut input)
            .take((MAX_IP_BODY + 1) as u64)
            .read_to_end(&mut body)
            .await?;
        ensure!(
            body.len() <= MAX_IP_BODY && input.get_ref().limit() > 0,
            "public IP response is too large"
        );
    }
    let ip = std::str::from_utf8(&body)?
        .trim()
        .parse::<Ipv4Addr>()
        .context("public IP service did not return an IPv4 address")?;
    ensure!(
        !ip.is_unspecified(),
        "public IP service returned an unspecified address"
    );
    Ok(ip)
}

fn commands(listen: SocketAddr, public_ip: Option<Ipv4Addr>, token: &str) -> ClientCommands {
    let needs_address = listen.ip().is_unspecified() && public_ip.is_none();
    let endpoint = if !listen.ip().is_unspecified() {
        listen.to_string()
    } else if let Some(ip) = public_ip {
        SocketAddr::from((ip, listen.port())).to_string()
    } else {
        format!("relay.example:{}", listen.port())
    };
    let windows = format!(
        "curl.exe -fL {RELEASE_ROOT}/fast-dump-x86_64-pc-windows-msvc.exe -o fast-dump.exe && \
         .\\fast-dump.exe --join-relay \"{endpoint}\" --access-token \"{token}\""
    );
    let linux = format!(
        "curl -fL {RELEASE_ROOT}/fast-dump-x86_64-unknown-linux-gnu -o fast-dump && \
         chmod u+x fast-dump && ./fast-dump --join-relay '{endpoint}' --access-token '{token}'"
    );
    ClientCommands {
        endpoint,
        needs_address,
        windows,
        linux,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_embed_release_urls_and_access_token() {
        let token = "23456789ABCD";
        let public_ip = "203.0.113.10".parse().unwrap();
        for (bind, detected, expected_endpoint) in [
            ("0.0.0.0:42171", Some(public_ip), "203.0.113.10:42171"),
            ("0.0.0.0:9876", None, "relay.example:9876"),
            ("[::]:12345", None, "relay.example:12345"),
            ("127.0.0.1:9876", None, "127.0.0.1:9876"),
            ("[::1]:9876", None, "[::1]:9876"),
        ] {
            let output = commands(bind.parse().unwrap(), detected, token);
            assert_eq!(output.endpoint, expected_endpoint);
            assert_eq!(
                output.needs_address,
                expected_endpoint.starts_with("relay.example:")
            );
            assert_eq!(
                output.windows,
                format!(
                    "curl.exe -fL {RELEASE_ROOT}/fast-dump-x86_64-pc-windows-msvc.exe -o fast-dump.exe && \
                     .\\fast-dump.exe --join-relay \"{expected_endpoint}\" --access-token \"{token}\""
                )
            );
            assert_eq!(
                output.linux,
                format!(
                    "curl -fL {RELEASE_ROOT}/fast-dump-x86_64-unknown-linux-gnu -o fast-dump && \
                     chmod u+x fast-dump && ./fast-dump --join-relay '{expected_endpoint}' --access-token '{token}'"
                )
            );
        }
    }

    #[tokio::test]
    async fn reads_plain_ipv4_with_or_without_content_length() {
        for response in [
            "HTTP/1.1 200 OK\r\ncontent-length: 13\r\n\r\n203.0.113.10\n",
            "HTTP/1.0 200 OK\r\n\r\n203.0.113.10",
        ] {
            assert_eq!(
                read_ipv4_response(response.as_bytes()).await.unwrap(),
                Ipv4Addr::new(203, 0, 113, 10)
            );
        }
    }

    #[tokio::test]
    async fn rejects_invalid_public_ip_responses() {
        for response in [
            "HTTP/1.1 503 Unavailable\r\n\r\n203.0.113.10",
            "HTTP/1.0 200 OK\r\n\r\n::1",
            "HTTP/1.0 200 OK\r\n\r\n0.0.0.0",
            "HTTP/1.0 200 OK\r\n\r\n203.0.113.10' & exit",
            "HTTP/1.0 200 OK\r\nContent-Length: 13\r\n\r\n203.0.113.10",
            "HTTP/1.0 200 OK\r\nContent-Length: 2048\r\n\r\n203.0.113.10",
        ] {
            assert!(
                read_ipv4_response(response.as_bytes()).await.is_err(),
                "{response:?}"
            );
        }
    }
}
