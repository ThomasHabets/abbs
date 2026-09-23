use std::{net::SocketAddr, path::PathBuf};

use abbs::{BbsConfig, Callsign, RadioConfig, start};
use anyhow::{Result, ensure};
use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Radio {
    Agw,
    Mercury,
}

#[derive(Debug, Parser)]
#[command(version, about = "Amateur radio BBS over AGW, Mercury HF, and TCP")]
struct Cli {
    /// Callsign by which radio users reach this BBS.
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

    /// Prompt displayed before commands.
    #[arg(long, default_value = "> ")]
    prompt: String,

    /// Prompt displayed before each message-body line.
    #[arg(long, default_value = "> ")]
    body_prompt: String,

    /// Allow TCP clients to create outgoing AX.25 BBS connections.
    #[arg(long)]
    allow_tcp_connect: bool,

    /// Include this BBS as a seen AX.25 via hop for outgoing connections.
    #[arg(long)]
    connect_via: bool,

    /// TCP address for terminal clients.
    #[arg(long, default_value = "0.0.0.0:8000")]
    tcp_listen: SocketAddr,

    /// AGWPE/Direwolf endpoint.
    #[arg(long, default_value = "127.0.0.1:8010")]
    agw_addr: String,

    /// AX.25 port number exposed by the AGW endpoint.
    #[arg(long, default_value_t = 1)]
    agw_port: u8,

    /// Radio backend. TCP clients are supported with either backend.
    #[arg(long, value_enum, default_value = "agw")]
    radio: Radio,

    /// Mercury hostname or IP address, without a port or IPv6 brackets.
    #[arg(long, default_value = "127.0.0.1")]
    mercury_host: String,

    /// Mercury control TCP port.
    #[arg(long, default_value_t = 8300)]
    mercury_control_port: u16,

    /// Mercury data TCP port (independent of the control port).
    #[arg(long, default_value_t = 8301)]
    mercury_data_port: u16,
}

impl Cli {
    fn radio_config(&self) -> Result<RadioConfig> {
        Ok(match self.radio {
            Radio::Agw => RadioConfig::Agw {
                addr: self.agw_addr.clone(),
                port: self.agw_port,
                connect_via: self.connect_via,
            },
            Radio::Mercury => {
                ensure!(
                    !self.connect_via,
                    "--connect-via is unsupported with Mercury"
                );
                ensure!(
                    !self.allow_tcp_connect,
                    "--allow-tcp-connect is unsupported with Mercury"
                );
                RadioConfig::Mercury {
                    host: self.mercury_host.clone(),
                    control_port: self.mercury_control_port,
                    data_port: self.mercury_data_port,
                }
            }
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();
    let radio = cli.radio_config()?;
    let config = BbsConfig {
        callsign: Callsign::parse(&cli.callsign)?,
        database_path: cli.db,
        files_dir: cli.files_dir,
        uploads_dir: cli.uploads_dir,
        prompt: cli.prompt,
        body_prompt: cli.body_prompt,
        allow_tcp_connect: cli.allow_tcp_connect,
        tcp_listen: cli.tcp_listen,
        radio,
    };
    let bbs = start(config).await?;
    log::info!("TCP BBS listening on {}", bbs.tcp_addr());
    tokio::signal::ctrl_c().await?;
    bbs.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radio_defaults_and_mercury_endpoints() {
        let cli = Cli::try_parse_from(["abbs", "--callsign", "M0BBS"]).unwrap();
        assert!(matches!(
            cli.radio_config().unwrap(),
            RadioConfig::Agw {
                port: 1,
                connect_via: false,
                ..
            }
        ));
        let cli =
            Cli::try_parse_from(["abbs", "--callsign", "M0BBS", "--radio", "mercury"]).unwrap();
        assert!(
            matches!(cli.radio_config().unwrap(), RadioConfig::Mercury { host, control_port: 8300, data_port: 8301 } if host == "127.0.0.1")
        );
        let cli = Cli::try_parse_from([
            "abbs",
            "--callsign",
            "M0BBS",
            "--radio",
            "mercury",
            "--mercury-host",
            "modem.local",
            "--mercury-control-port",
            "9000",
            "--mercury-data-port",
            "9100",
        ])
        .unwrap();
        assert!(
            matches!(cli.radio_config().unwrap(), RadioConfig::Mercury { host, control_port: 9000, data_port: 9100 } if host == "modem.local")
        );
    }

    #[test]
    fn mercury_rejects_outgoing_agw_flags() {
        for flag in ["--connect-via", "--allow-tcp-connect"] {
            let cli =
                Cli::try_parse_from(["abbs", "--callsign", "M0BBS", "--radio", "mercury", flag])
                    .unwrap();
            assert!(cli.radio_config().is_err());
        }
    }
}
