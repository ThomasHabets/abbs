use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use abbs::{BbsConfig, Callsign, start};
use agw::{Call, Packet, Pid, Port, ViaHop, r#async::AGWServer};
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
        uploads_dir: None,
        prompt: "> ".into(),
        body_prompt: "> ".into(),
        allow_tcp_connect: false,
        connect_via: false,
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

async fn next_packet(server: &mut AGWServer, description: &str) -> Result<Packet> {
    timeout(Duration::from_secs(2), server.recv())
        .await
        .with_context(|| format!("timed out waiting for {description}"))?
        .map_err(Into::into)
}

async fn expect_outbound_registration(server: &mut AGWServer, source: &Call) -> Result<()> {
    loop {
        match next_packet(server, "outgoing callsign registration").await? {
            Packet::RegisterCallsign(port, call) => {
                assert_eq!(port, Port(1));
                assert_eq!(call, source.clone());
                server
                    .send(&Packet::RegisterCallsignReply {
                        port,
                        call,
                        success: true,
                    })
                    .await?;
                return Ok(());
            }
            Packet::Data { .. } => {}
            packet => bail!("unexpected packet before outgoing registration: {packet:?}"),
        }
    }
}

async fn expect_connect(server: &mut AGWServer, source: &Call, destination: &Call) -> Result<()> {
    loop {
        match next_packet(server, "Connect").await? {
            Packet::Connect {
                port,
                pid,
                src,
                dst,
            } => {
                assert_eq!(port, Port(1));
                assert_eq!(pid, Pid(0xf0));
                assert_eq!(src, source.clone());
                assert_eq!(dst, destination.clone());
                return Ok(());
            }
            Packet::Data { .. } => {}
            packet => bail!("unexpected packet before Connect: {packet:?}"),
        }
    }
}

async fn expect_connect_via(
    server: &mut AGWServer,
    bbs: &Call,
    source: &Call,
    destination: &Call,
) -> Result<()> {
    loop {
        match next_packet(server, "seen ConnectVia").await? {
            Packet::ConnectViaMarked {
                port,
                pid,
                src,
                dst,
                via,
            } => {
                assert_eq!(port, Port(1));
                assert_eq!(pid, Pid(0xf0));
                assert_eq!(src, source.clone());
                assert_eq!(dst, destination.clone());
                assert_eq!(via, vec![ViaHop::seen(bbs.clone())]);
                return Ok(());
            }
            Packet::Data { .. } => {}
            packet => bail!("unexpected packet before ConnectVia: {packet:?}"),
        }
    }
}

async fn expect_data(
    server: &mut AGWServer,
    source: &Call,
    destination: &Call,
    data: &[u8],
) -> Result<()> {
    loop {
        match next_packet(server, "relayed data").await? {
            Packet::Data {
                src,
                dst,
                data: actual,
                ..
            } if src == *source && dst == *destination && actual == data => return Ok(()),
            Packet::Data { .. } => {}
            packet => bail!("unexpected packet while relaying data: {packet:?}"),
        }
    }
}

async fn expect_disconnect(
    server: &mut AGWServer,
    source: &Call,
    destination: &Call,
) -> Result<()> {
    loop {
        match next_packet(server, "remote disconnect").await? {
            Packet::Disconnect { src, dst, .. } if src == *source && dst == *destination => {
                return Ok(());
            }
            Packet::Data { .. } => {}
            packet => bail!("unexpected packet while disconnecting remote BBS: {packet:?}"),
        }
    }
}

async fn serve_ax25_remote_bbs(
    agw_listener: TcpListener,
    bbs_call: Call,
    client_call: Call,
    source_call: Call,
    destination_call: Call,
    connect_via: bool,
) -> Result<()> {
    let (stream, _) = agw_listener.accept().await?;
    let mut server = AGWServer::new(stream);
    let Packet::RegisterCallsign(port, call) =
        next_packet(&mut server, "BBS callsign registration").await?
    else {
        bail!("expected BBS callsign registration");
    };
    assert_eq!(port, Port(1));
    assert_eq!(call, bbs_call);
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
            src: client_call.clone(),
            dst: bbs_call.clone(),
        })
        .await?;
    let _ = read_bbs_output_until(&mut server, "Welcome, M0REM-4.").await?;

    server
        .send(&Packet::Data {
            port: Port(1),
            pid: Pid(0xf0),
            src: client_call.clone(),
            dst: bbs_call.clone(),
            data: b"CONNECT M0DEST 9\r".to_vec(),
        })
        .await?;
    expect_outbound_registration(&mut server, &source_call).await?;
    if connect_via {
        expect_connect_via(&mut server, &bbs_call, &source_call, &destination_call).await?;
    } else {
        expect_connect(&mut server, &source_call, &destination_call).await?;
    }
    server
        .send(&Packet::ConnectionEstablished {
            port: Port(1),
            pid: Pid(0xf0),
            src: destination_call.clone(),
            dst: source_call.clone(),
        })
        .await?;
    let _ = read_bbs_output_until(
        &mut server,
        "Connected. Enter ~. on a line by itself to return here.",
    )
    .await?;

    server
        .send(&Packet::Data {
            port: Port(1),
            pid: Pid(0xf0),
            src: client_call.clone(),
            dst: bbs_call.clone(),
            data: b"HELLO\r".to_vec(),
        })
        .await?;
    expect_data(&mut server, &source_call, &destination_call, b"HELLO\r").await?;
    server
        .send(&Packet::Data {
            port: Port(1),
            pid: Pid(0xf0),
            src: destination_call.clone(),
            dst: source_call.clone(),
            data: b"REMOTE\r".to_vec(),
        })
        .await?;
    expect_data(&mut server, &bbs_call, &client_call, b"REMOTE\r").await?;

    server
        .send(&Packet::Data {
            port: Port(1),
            pid: Pid(0xf0),
            src: client_call,
            dst: bbs_call,
            data: b"~.\r".to_vec(),
        })
        .await?;
    expect_disconnect(&mut server, &source_call, &destination_call).await?;
    let _ = read_bbs_output_until(&mut server, "Disconnected from remote BBS.").await?;
    let _ = read_bbs_output_until(&mut server, "> ").await?;
    Ok(())
}

async fn run_ax25_connect_test(connect_via: bool) -> Result<()> {
    let agw_listener = TcpListener::bind("127.0.0.1:0").await?;
    let agw_addr = agw_listener.local_addr()?.to_string();
    let fake_agw = tokio::spawn(serve_ax25_remote_bbs(
        agw_listener,
        "M0BBS".parse()?,
        "M0REM-4".parse()?,
        "M0REM-9".parse()?,
        "M0DEST".parse()?,
        connect_via,
    ));

    let database_path = database_path();
    let files_dir = database_path.with_extension("files");
    let bbs = start(BbsConfig {
        callsign: Callsign::parse("M0BBS")?,
        database_path: database_path.clone(),
        files_dir: files_dir.clone(),
        uploads_dir: None,
        prompt: "> ".into(),
        body_prompt: "> ".into(),
        allow_tcp_connect: false,
        connect_via,
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

#[tokio::test]
async fn connects_an_ax25_client_to_a_remote_bbs() -> Result<()> {
    run_ax25_connect_test(false).await
}

#[tokio::test]
async fn adds_a_seen_bbs_via_hop_when_enabled() -> Result<()> {
    run_ax25_connect_test(true).await
}
