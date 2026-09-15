use std::{net::SocketAddr, path::PathBuf};

use abbs::{BbsConfig, Callsign, start};
use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about = "Amateur radio BBS over AX.25 and TCP")]
struct Cli {
    /// Callsign by which AX.25 users reach this BBS.
    #[arg(long)]
    callsign: String,

    /// SQLite database path.
    #[arg(long, default_value = "abbs.sqlite3")]
    db: PathBuf,

    /// Directory containing files available for download.
    #[arg(long, default_value = "files")]
    files_dir: PathBuf,

    /// Directory for received ZMODEM uploads. Defaults to --files-dir.
    #[arg(long)]
    uploads_dir: Option<PathBuf>,

    /// Prompt displayed before command and message-body input.
    #[arg(long, default_value = "> ")]
    prompt: String,

    /// TCP address for terminal clients.
    #[arg(long, default_value = "0.0.0.0:8000")]
    tcp_listen: SocketAddr,

    /// AGWPE/Direwolf endpoint.
    #[arg(long, default_value = "127.0.0.1:8010")]
    agw_addr: String,

    /// AX.25 port number exposed by the AGW endpoint.
    #[arg(long, default_value_t = 1)]
    agw_port: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let config = BbsConfig {
        callsign: Callsign::parse(&cli.callsign)?,
        database_path: cli.db,
        files_dir: cli.files_dir,
        uploads_dir: cli.uploads_dir,
        prompt: cli.prompt,
        tcp_listen: cli.tcp_listen,
        agw_addr: cli.agw_addr,
        agw_port: cli.agw_port,
    };
    let bbs = start(config).await?;
    log::info!("TCP BBS listening on {}", bbs.tcp_addr());
    tokio::signal::ctrl_c().await?;
    bbs.shutdown().await
}
