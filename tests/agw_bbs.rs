use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use abbs::{BbsConfig, Callsign, start};
use agw::{Call, Packet, Pid, Port, r#async::AGWServer};
use anyhow::{Context, Result, bail};
use tokio::{net::TcpListener, time::timeout};

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "abbs-agw-test-{}-{}.sqlite3",
        std::process::id(),
        NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn read_bbs_output_until(server: &mut AGWServer, expected: &str) -> Result<String> {
    let mut output = String::new();
    loop {
        let packet = timeout(Duration::from_secs(2), server.recv())
            .await
            .context("timed out waiting for BBS AX.25 output")??;
        if let Packet::Data { data, .. } = packet {
            output.push_str(&String::from_utf8(data).context("BBS sent invalid UTF-8")?);
            if output.contains(expected) {
                return Ok(output);
            }
        }
    }
}

#[tokio::test]
async fn accepts_an_ax25_connection_and_runs_the_shared_command_session() -> Result<()> {
    let agw_listener = TcpListener::bind("127.0.0.1:0").await?;
    let agw_addr = agw_listener.local_addr()?.to_string();
    let bbs_call: Call = "M0BBS".parse()?;
    let remote_call: Call = "M0REMOTE".parse()?;

    let fake_agw = tokio::spawn(async move {
        let (stream, _) = agw_listener.accept().await?;
        let mut server = AGWServer::new(stream);
        let registration = server.recv().await?;
        let Packet::RegisterCallsign(port, call) = registration else {
            bail!("expected callsign registration, got {registration:?}");
        };
        assert_eq!(port, Port(1));
        assert_eq!(call.to_string(), "M0BBS");
        server
            .send(&Packet::RegisterCallsignReply {
                port,
                call,
                success: true,
            })
            .await?;
        server
            .send(&Packet::IncomingConnect {
                port: Port(1),
                pid: Pid(0),
                src: remote_call.clone(),
                dst: bbs_call.clone(),
            })
            .await?;

        let welcome = read_bbs_output_until(&mut server, "Welcome, M0REMOTE.").await?;
        assert!(welcome.contains("Welcome to M0BBS amateur radio BBS."));

        server
            .send(&Packet::Data {
                port: Port(1),
                pid: Pid(0xf0),
                src: remote_call,
                dst: bbs_call,
                data: b"HELP\r".to_vec(),
            })
            .await?;
        let help = read_bbs_output_until(&mut server, "  QUIT").await?;
        assert!(help.contains("Commands:"));
        assert!(help.contains("SEND <callsign|ALL>"));
        Ok::<(), anyhow::Error>(())
    });

    let database_path = database_path();
    let files_dir = database_path.with_extension("files");
    let bbs = start(BbsConfig {
        callsign: Callsign::parse("M0BBS")?,
        database_path: database_path.clone(),
        files_dir: files_dir.clone(),
        tcp_listen: "127.0.0.1:0".parse()?,
        agw_addr,
        agw_port: 1,
    })
    .await?;

    timeout(Duration::from_secs(5), fake_agw)
        .await
        .context("fake AGW test did not finish")???;
    bbs.shutdown().await?;
    let _ = fs::remove_file(&database_path);
    let _ = fs::remove_file(database_path.with_extension("sqlite3-wal"));
    let _ = fs::remove_file(database_path.with_extension("sqlite3-shm"));
    let _ = fs::remove_dir_all(files_dir);
    Ok(())
}
