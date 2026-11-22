use std::net::SocketAddr;

const RELEASE_ROOT: &str = "https://github.com/BURAKKU1XD/fast-dump/releases/latest/download";

const WINDOWS: &str = r#"
    param($url, $relay, $invite);
    $dir = [IO.Directory]::CreateDirectory((Join-Path ([IO.Path]::GetTempPath()) ("fast-dump-" + [Guid]::NewGuid().ToString("N")))).FullName;
    $checksum = Join-Path $dir "fast-dump.exe.sha256";
    $client = Join-Path $dir "fast-dump.exe";
    try {
        Invoke-WebRequest -UseBasicParsing -ErrorAction Stop -Uri "$url.sha256" -OutFile $checksum;
        $digest = ((Get-Content -LiteralPath $checksum -Raw -ErrorAction Stop).Trim() -split "\s+", 2)[0];
        if ($digest -notmatch "^[0-9a-fA-F]{64}$") { throw "Invalid release checksum." };
        Invoke-WebRequest -UseBasicParsing -ErrorAction Stop -Uri $url -OutFile $client;
        if ((Get-FileHash -LiteralPath $client -Algorithm SHA256 -ErrorAction Stop).Hash -ne $digest) { throw "Release checksum mismatch." };
        & $client --join-relay $relay --invitation $invite;
        if ($LASTEXITCODE -ne 0) { throw "fast-dump exited with code $LASTEXITCODE." };
    } finally {
        [IO.File]::Delete($client);
        [IO.File]::Delete($checksum);
        [IO.Directory]::Delete($dir);
    }
"#;

const LINUX: &str = r#"
    set -eu;
    dir=$(mktemp -d);
    client="$dir/fast-dump";
    checksum="$dir/fast-dump.sha256";
    cleanup() { rm -f -- "$client" "$checksum"; rmdir -- "$dir"; };
    trap cleanup EXIT;
    curl -fLsS --proto "=https" --output "$checksum" "$1.sha256";
    read -r digest _ < "$checksum";
    if ! [[ $digest =~ ^[0-9a-fA-F]{64}$ ]]; then printf "%s\n" "Invalid release checksum." >&2; exit 1; fi;
    curl -fLsS --proto "=https" --output "$client" "$1";
    actual=$(sha256sum < "$client");
    if [[ ${actual%% *} != "${digest,,}" ]]; then printf "%s\n" "Release checksum mismatch." >&2; exit 1; fi;
    chmod u+x -- "$client";
    "$client" --join-relay "$2" --invitation "$3"
"#;

fn single_line(script: &str) -> String {
    script
        .trim()
        .lines()
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" ")
}

pub(super) fn instructions(listen: SocketAddr, invitation: &str) -> String {
    let (endpoint, replacement) = if listen.ip().is_unspecified() {
        (
            format!("relay.example:{}", listen.port()),
            "Replace relay.example with the IP or DNS name reachable by the clients.\n",
        )
    } else {
        (listen.to_string(), "")
    };
    let windows_url = format!("{RELEASE_ROOT}/fast-dump-x86_64-pc-windows-msvc.exe");
    let linux_url = format!("{RELEASE_ROOT}/fast-dump-x86_64-unknown-linux-gnu");
    let windows = single_line(WINDOWS);
    let linux = single_line(LINUX);
    format!(
        "\nClient setup\n\
         {replacement}\n\
         Windows x86-64 (PowerShell)\n\
         & {{ {windows} }} '{windows_url}' '{endpoint}' '{invitation}'\n\n\
         Linux x86-64 (Bash)\n\
         bash -c '{linux}' -- '{linux_url}' '{endpoint}' '{invitation}'"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_embed_release_urls_and_relay_invitation() {
        let invitation = super::super::invitation([0xAB; 32], "23456789ABCD");
        for (bind, expected_endpoint) in [
            ("0.0.0.0:9876", "relay.example:9876"),
            ("[::]:12345", "relay.example:12345"),
            ("[::1]:9876", "[::1]:9876"),
        ] {
            let output = instructions(bind.parse().unwrap(), &invitation);
            let windows = output.lines().find(|line| line.starts_with("& {")).unwrap();
            let linux = output
                .lines()
                .find(|line| line.starts_with("bash -c "))
                .unwrap();
            let args = format!("'{expected_endpoint}' '{invitation}'");
            assert!(windows.ends_with(&args));
            assert!(linux.ends_with(&args));
            assert!(windows.contains("--join-relay $relay --invitation $invite"));
            assert!(linux.contains(r#"--join-relay "$2" --invitation "$3""#));
            assert!(
                windows.contains("/releases/latest/download/fast-dump-x86_64-pc-windows-msvc.exe'")
            );
            assert!(
                linux.contains("/releases/latest/download/fast-dump-x86_64-unknown-linux-gnu'")
            );
        }
    }
}
