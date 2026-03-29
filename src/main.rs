use anyhow::Result;
use clap::Parser;
use fast_dump::cli::Cli;
use fast_dump::network::transport::establish_session;

// Пока binary делает только одно:
// поднимает transport-слой и убеждается, что вторая сторона доступна.
// Когда появится полноценный sender/receiver runtime, именно отсюда
// в него пойдут уже провалидированные роль и транспортный режим.
fn main() -> Result<()> {
    let cli = Cli::parse();
    // Роль определяется направлением данных, а не тем, кто был TCP-клиентом.
    let role = cli.command.role();
    // Для runtime push/pull уже сведены к строгому enum listen/connect.
    let transport_mode = cli.command.transport_mode()?;
    let session = establish_session(role, transport_mode)?;

    println!(
        "{} connected to {} at {}",
        session.local_role, session.peer_role, session.peer_addr
    );

    Ok(())
}
