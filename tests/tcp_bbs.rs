use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use abbs::{BbsConfig, BbsHandle, Callsign, RadioConfig, start};
use agw::{Call, Packet, Pid, Port, r#async::AGWServer};
use anyhow::{Context, Result, bail};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot, time::timeout};

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

mod common;
use common::{Client, receive_zmodem_download, send_zmodem_upload};

fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "abbs-tcp-test-{}-{}.sqlite3",
        std::process::id(),
        NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn start_test_bbs() -> Result<(BbsHandle, PathBuf, PathBuf)> {
    start_test_bbs_with_uploads(None).await
}

async fn start_test_bbs_with_uploads(
    uploads_dir: Option<PathBuf>,
) -> Result<(BbsHandle, PathBuf, PathBuf)> {
    start_test_bbs_with_options(uploads_dir, "> ".into(), "> ".into()).await
}

async fn start_test_bbs_with_options(
    uploads_dir: Option<PathBuf>,
    prompt: String,
    body_prompt: String,
) -> Result<(BbsHandle, PathBuf, PathBuf)> {
    let database_path = database_path();
    let files_dir = database_path.with_extension("files");
    let bbs = start(BbsConfig {
        callsign: Callsign::parse("M0BBS")?,
        database_path: database_path.clone(),
        files_dir: files_dir.clone(),
        uploads_dir,
        prompt,
        body_prompt,
        allow_tcp_connect: false,
        tcp_listen: "127.0.0.1:0".parse()?,
        // No AGW server is needed for TCP functionality; the BBS must remain
        // available while its radio listener retries.
        radio: RadioConfig::Agw {
            addr: "127.0.0.1:9".into(),
            port: 1,
            connect_via: false,
        },
    })
    .await?;
    Ok((bbs, database_path, files_dir))
}

fn remove_database(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
    let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
}

async fn serve_tcp_remote_bbs(
    agw_listener: TcpListener,
    bbs_call: Call,
    source_call: Call,
    destination_call: Call,
    endpoint_ready_tx: oneshot::Sender<()>,
) -> Result<()> {
    let (stream, _) = agw_listener.accept().await?;
    let mut server = AGWServer::new(stream);
    let Packet::RegisterCallsign(port, call) = server.recv().await? else {
        bail!("expected BBS callsign registration");
    };
    assert_eq!(port, Port(2));
    assert_eq!(call, bbs_call);
    server
        .send(&Packet::RegisterCallsignReply {
            port,
            call,
            success: true,
        })
        .await?;
    let _ = endpoint_ready_tx.send(());

    let Packet::RegisterCallsign(port, call) = server.recv().await? else {
        bail!("expected outgoing callsign registration");
    };
    // AGW's packet decoder represents the X frame's wire port 0 as Port(1).
    // This differs from the configured AX.25 connection port below.
    assert_eq!(port, Port(1));
    assert_eq!(call, source_call);
    server
        .send(&Packet::RegisterCallsignReply {
            port,
            call,
            success: true,
        })
        .await?;

    let Packet::Connect {
        port,
        pid,
        src,
        dst,
    } = server.recv().await?
    else {
        bail!("expected an outgoing Connect request");
    };
    assert_eq!(port, Port(2));
    assert_eq!(pid, Pid(0xf0));
    assert_eq!(src, source_call);
    assert_eq!(dst, destination_call);
    server
        .send(&Packet::ConnectionEstablished {
            port,
            pid,
            src: destination_call.clone(),
            dst: source_call.clone(),
        })
        .await?;

    let Packet::Disconnect { src, dst, .. } = server.recv().await? else {
        bail!("expected outgoing remote disconnect");
    };
    assert_eq!(src, source_call);
    assert_eq!(dst, destination_call);
    Ok(())
}

#[tokio::test]
async fn tcp_connect_is_opt_in_and_uses_the_requested_ssid() -> Result<()> {
    let agw_listener = TcpListener::bind("127.0.0.1:0").await?;
    let agw_addr = agw_listener.local_addr()?.to_string();
    let (endpoint_ready_tx, endpoint_ready_rx) = oneshot::channel();
    let fake_agw = tokio::spawn(serve_tcp_remote_bbs(
        agw_listener,
        "M0BBS".parse()?,
        "M0ALICE-9".parse()?,
        "M0DEST".parse()?,
        endpoint_ready_tx,
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
        allow_tcp_connect: true,
        tcp_listen: "127.0.0.1:0".parse()?,
        radio: RadioConfig::Agw {
            addr: agw_addr,
            port: 2,
            connect_via: false,
        },
    })
    .await?;

    let test_result = async {
        timeout(Duration::from_secs(2), endpoint_ready_rx)
            .await
            .context("AGW endpoint did not become ready")??;
        let mut client = Client::connect(bbs.tcp_addr(), "m0alice", b"\r\n").await?;
        client.send_line("CONNECT M0DEST 9").await?;
        assert!(
            client
                .read_until(b"Connected. Enter ~. on a line by itself to return here.")
                .await?
                .contains("Connecting M0ALICE-9 to M0DEST...")
        );
        client.send_line("~.").await?;
        assert!(
            client
                .read_until(b"> ")
                .await?
                .contains("Disconnected from remote BBS.")
        );
        timeout(Duration::from_secs(2), fake_agw)
            .await
            .context("fake AGW test did not finish")???;
        Ok(())
    }
    .await;

    let shutdown_result = bbs.shutdown().await;
    remove_database(&database_path);
    let _ = fs::remove_dir_all(files_dir);
    shutdown_result?;
    test_result
}

#[tokio::test]
async fn tcp_clients_can_exchange_private_and_public_messages_with_cr_and_crlf() -> Result<()> {
    let (bbs, database_path, files_dir) = start_test_bbs().await?;
    let address = bbs.tcp_addr();
    fs::write(files_dir.join("bulletin.txt"), b"CQ CQ")?;

    let mut alice = Client::connect(address, "m0alice", b"\r").await?;
    let mut bob = Client::connect(address, "m0bob", b"\r\n").await?;
    let mut eve = Client::connect(address, "m0eve", b"\r\n").await?;

    let logins = eve.command("LOGINS").await?;
    assert!(logins.contains("M0ALICE via TCP"));
    assert!(logins.contains("M0BOB via TCP"));
    assert!(logins.contains("M0EVE via TCP"));
    assert!(!logins.contains("127.0.0.1"));

    let info = eve.command("INFO").await?;
    assert!(info.contains(&format!("ABBS {}", env!("CARGO_PKG_VERSION"))));
    assert!(info.contains("BBS callsign: M0BBS"));

    let files = eve.command("FILES").await?;
    assert!(files.contains("bulletin.txt (5 bytes)"));
    assert!(
        eve.command("CONNECT M0DEST 1")
            .await?
            .contains("CONNECT is disabled for TCP clients.")
    );
    assert!(
        eve.command("DOWNLOAD ../bulletin.txt")
            .await?
            .contains("Invalid file name.")
    );

    let saved = alice
        .send_message(
            "M0BOB-7",
            "Private subject",
            &["first private line", "second private line"],
        )
        .await?;
    assert!(saved.contains("Message #1 saved."));

    let bob_list = bob.command("LIST").await?;
    assert!(bob_list.contains("Private subject"));
    let bob_message = bob.command("READ 1").await?;
    assert!(bob_message.contains("first private line\r\nsecond private line"));

    let eve_list = eve.command("LIST").await?;
    assert!(!eve_list.contains("Private subject"));
    assert!(eve.command("READ 1").await?.contains("Message not found."));

    let saved = alice
        .send_message("ALL", "Public subject", &["heard by everyone"])
        .await?;
    assert!(saved.contains("Message #2 saved."));

    let bob_list = bob.command("LIST").await?;
    assert!(bob_list.contains("Public subject"));
    let eve_public = eve.command("READ 2").await?;
    assert!(eve_public.contains("To: ALL"));
    assert!(eve_public.contains("heard by everyone"));

    let mut alice_ssid = Client::connect(address, "m0alice-3", b"\r\n").await?;
    let alice_sent = alice_ssid.command("SENT").await?;
    assert!(alice_sent.contains("Private subject"));
    assert!(alice_sent.contains("Public subject"));
    assert!(alice_sent.contains("TO M0BOB-7"));
    assert!(alice_sent.contains("TO ALL"));
    assert!(bob.command("SENT").await?.contains("No sent messages."));

    assert!(
        eve.command("DELETE 1")
            .await?
            .contains("Message not found or cannot be deleted.")
    );
    assert!(bob.command("READ 1").await?.contains("Private subject"));

    assert!(bob.command("DELETE 1").await?.contains("Message deleted."));
    assert!(bob.command("READ 1").await?.contains("Message not found."));
    let alice_sent = alice_ssid.command("SENT").await?;
    assert!(!alice_sent.contains("Private subject"));
    assert!(alice_sent.contains("Public subject"));

    assert!(
        bob.command("DELETE 2")
            .await?
            .contains("Message not found or cannot be deleted.")
    );
    assert!(
        alice_ssid
            .command("DELETE 2")
            .await?
            .contains("Message deleted.")
    );
    assert!(eve.command("READ 2").await?.contains("Message not found."));
    assert!(
        alice_ssid
            .command("SENT")
            .await?
            .contains("No sent messages.")
    );

    alice.send_line("BYE").await?;
    assert!(
        alice
            .read_until(b"Goodbye.\r\n")
            .await?
            .contains("Goodbye.")
    );
    bob.send_line("EXIT").await?;
    assert!(bob.read_until(b"Goodbye.\r\n").await?.contains("Goodbye."));

    bbs.shutdown().await?;
    remove_database(&database_path);
    let _ = fs::remove_dir_all(files_dir);
    Ok(())
}

#[tokio::test]
async fn successful_download_stays_silent_until_the_next_command() -> Result<()> {
    let (bbs, database_path, files_dir) = start_test_bbs().await?;
    let test_result = async {
        fs::write(files_dir.join("bulletin.txt"), b"CQ CQ")?;
        let mut client = Client::connect(bbs.tcp_addr(), "m0alice", b"\r\n").await?;

        client.send_line("DOWNLOAD bulletin.txt").await?;
        assert!(
            client
                .read_until(b"Starting ZMODEM download.\r\n")
                .await?
                .contains("Starting ZMODEM download.")
        );
        receive_zmodem_download(&mut client, b"CQ CQ").await?;

        let mut buffer = [0_u8; 32];
        match timeout(Duration::from_millis(100), client.stream.read(&mut buffer)).await {
            Err(_) => {}
            Ok(Ok(read)) => panic!(
                "BBS sent terminal data before the next command: {:?}",
                &buffer[..read]
            ),
            Ok(Err(error)) => return Err(error.into()),
        }

        assert!(client.command("LIST").await?.contains("No messages."));
        Ok(())
    }
    .await;

    let shutdown_result = bbs.shutdown().await;
    remove_database(&database_path);
    let _ = fs::remove_dir_all(files_dir);
    shutdown_result?;
    test_result
}

#[tokio::test]
async fn tcp_client_zmodem_uploads_a_file_and_returns_to_commands() -> Result<()> {
    let uploads_dir = database_path().with_extension("uploads");
    let (bbs, database_path, files_dir) =
        start_test_bbs_with_uploads(Some(uploads_dir.clone())).await?;
    let test_result = async {
        let mut client = Client::connect(bbs.tcp_addr(), "m0alice", b"\r\n").await?;

        send_zmodem_upload(&mut client, b"uplink.txt", b"CQ from ZMODEM").await?;
        assert_eq!(fs::read(uploads_dir.join("uplink.txt"))?, b"CQ from ZMODEM");
        assert!(!files_dir.join("uplink.txt").exists());
        let files = client.command("FILES").await?;
        anyhow::ensure!(
            files.contains("No files available."),
            "unexpected FILES response: {files:?}"
        );
        Ok(())
    }
    .await;

    let shutdown_result = bbs.shutdown().await;
    remove_database(&database_path);
    let _ = fs::remove_dir_all(files_dir);
    let _ = fs::remove_dir_all(uploads_dir);
    shutdown_result?;
    test_result
}

#[tokio::test]
async fn tcp_client_uses_a_configured_prompt() -> Result<()> {
    let (bbs, database_path, files_dir) =
        start_test_bbs_with_options(None, "abbs> ".into(), "body> ".into()).await?;
    let test_result = async {
        let mut client =
            Client::connect_with_prompts(bbs.tcp_addr(), "m0alice", b"\r\n", b"abbs> ", b"body> ")
                .await?;
        assert!(
            client
                .send_message("ALL", "Prompt test", &["message body"])
                .await?
                .contains("Message #1 saved.")
        );
        assert!(client.command("LIST").await?.contains("Prompt test"));
        Ok(())
    }
    .await;

    let shutdown_result = bbs.shutdown().await;
    remove_database(&database_path);
    let _ = fs::remove_dir_all(files_dir);
    shutdown_result?;
    test_result
}
