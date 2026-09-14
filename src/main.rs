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
        tcp_listen: cli.tcp_listen,
        agw_addr: cli.agw_addr,
        agw_port: cli.agw_port,
    };
    let bbs = start(config).await?;
    log::info!("TCP BBS listening on {}", bbs.tcp_addr());
    tokio::signal::ctrl_c().await?;
    bbs.shutdown().await
}
