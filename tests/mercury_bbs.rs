mod common;

use std::time::Duration;

use abbs::{BbsConfig, BbsHandle, Callsign, RadioConfig, start};
use anyhow::{Context, Result, ensure};
use common::{Client, receive_zmodem_download, send_zmodem_upload};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};

struct TestBbs {
    bbs: BbsHandle,
    control: TcpListener,
    data: TcpListener,
    directory: TempDir,
}

impl TestBbs {
    async fn start() -> Result<Self> {
        let control = TcpListener::bind("127.0.0.1:0").await?;
        let data = TcpListener::bind("127.0.0.1:0").await?;
        let directory = TempDir::new()?;
        let bbs = start(BbsConfig {
            callsign: Callsign::parse("M0BBS")?,
            database_path: directory.path().join("bbs.sqlite3"),
            files_dir: directory.path().join("files"),
            uploads_dir: Some(directory.path().join("uploads")),
            prompt: "> ".into(),
            body_prompt: "body> ".into(),
            allow_tcp_connect: false,
            tcp_listen: "127.0.0.1:0".parse()?,
            radio: RadioConfig::Mercury {
                host: "127.0.0.1".into(),
                control_port: control.local_addr()?.port(),
                data_port: data.local_addr()?.port(),
            },
        })
        .await?;
        Ok(Self {
            bbs,
            control,
            data,
            directory,
        })
    }

    async fn sockets(&self) -> Result<Modem> {
        let (control, _) = timeout(Duration::from_secs(8), self.control.accept()).await??;
        let (data, _) = timeout(Duration::from_secs(2), self.data.accept()).await??;
        let mut client = Client::from_stream(data, b"\r");
        client.body_prompt = b"body> ".to_vec();
        Ok(Modem {
            control: BufReader::new(control),
            client,
        })
    }

    async fn accept(&self) -> Result<Modem> {
        let mut modem = self.sockets().await?;
        for command in ["MYCALL M0BBS", "PUBLIC OFF", "LISTEN ON"] {
            modem.expect_command(command).await?;
            modem.event("OK").await?;
        }
        Ok(modem)
    }
}

struct Modem {
    control: BufReader<TcpStream>,
    client: Client,
}

impl Modem {
    async fn expect_command(&mut self, expected: &str) -> Result<()> {
        let mut bytes = Vec::new();
        let count = timeout(
            Duration::from_secs(2),
            self.control.read_until(b'\r', &mut bytes),
        )
        .await
        .context("waiting for Mercury command")??;
        ensure!(
            count != 0,
            "control socket closed while waiting for {expected}"
        );
        assert_eq!(String::from_utf8(bytes)?.trim(), expected);
        Ok(())
    }

    async fn event(&mut self, event: &str) -> Result<()> {
        self.control
            .get_mut()
            .write_all(format!("{event}\r").as_bytes())
            .await?;
        Ok(())
    }

    async fn connect(&mut self, call: &str) -> Result<()> {
        self.event(&format!("CONNECTED {call} M0BBS 2300")).await?;
        let greeting = self.client.read_until(b"> ").await?;
        assert!(
            greeting.contains(&format!("Welcome, {call}.")),
            "{greeting:?}"
        );
        assert!(!greeting.contains("Callsign:"));
        Ok(())
    }

    async fn quit(&mut self, command: &str) -> Result<()> {
        self.client.send_line(command).await?;
        self.client.read_until(b"Goodbye.\r\n").await?;
        self.expect_command("DISCONNECT").await?;
        self.event("OK").await?;
        self.event("DISCONNECTED").await
    }

    async fn closed(&mut self) -> Result<()> {
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(2), self.control.read(&mut byte)).await??,
            0
        );
        assert_eq!(
            timeout(Duration::from_secs(2), self.client.stream.read(&mut byte)).await??,
            0
        );
        Ok(())
    }
}

#[tokio::test]
async fn mercury_sessions_share_mail_with_tcp_and_reuse_the_modem_sockets() -> Result<()> {
    let test = TestBbs::start().await?;
    let mut modem = test.accept().await?;
    let mut byte = [0];
    assert!(
        timeout(
            Duration::from_millis(30),
            modem.client.stream.read(&mut byte)
        )
        .await
        .is_err()
    );
    modem.connect("M0ALICE-1").await?;
    let mut tcp = Client::connect(test.bbs.tcp_addr(), "M0BOB", b"\r\n").await?;

    modem.client.send_line("HELP").await?;
    let help = modem
        .client
        .read_until(b"QUIT, BYE, EXIT      Disconnect\r\n")
        .await?;
    modem.client.read_until(b"> ").await?;
    assert!(!help.contains("HEARD"));
    assert!(!help.contains("CONNECT <"));
    for client in [&mut modem.client, &mut tcp] {
        assert!(
            client
                .command("HEARD")
                .await?
                .contains("HEARD is unsupported with Mercury.")
        );
        assert!(
            client
                .command("CONNECT M0DEST 3")
                .await?
                .contains("CONNECT is unsupported with Mercury.")
        );
    }
    let saved = modem
        .client
        .send_message("M0BOB-2", "Mercury mail", &["hello over HF"])
        .await?;
    assert!(saved.contains("Message #1 saved."));
    assert!(tcp.command("READ 1").await?.contains("hello over HF"));
    let logins = tcp.command("LOGINS").await?;
    assert!(logins.contains("M0ALICE-1 via MERCURY"));
    assert!(logins.contains("M0BOB via TCP"));
    assert!(!logins.contains("127.0.0.1"));

    // Disconnect while composition is waiting for a subject; buffered partial
    // input must not become the next caller's first command.
    modem.client.send_line("SEND ALL").await?;
    modem.client.read_until(b"Subject: ").await?;
    modem.client.stream.write_all(b"unfinished subject").await?;
    modem.event("DISCONNECTED").await?;
    modem.connect("M0EVE").await?;
    modem.client.line_ending = b"\r\n";
    assert!(modem.client.command("LIST").await?.contains("No messages."));
    assert!(
        modem
            .client
            .command("READ 1")
            .await?
            .contains("Message not found.")
    );
    assert!(
        modem
            .client
            .command("INFO")
            .await?
            .contains("BBS callsign: M0BBS")
    );
    modem.quit("BYE").await?;
    modem.connect("M0ALICE-9").await?;
    assert!(modem.client.command("SENT").await?.contains("Mercury mail"));
    assert!(
        modem
            .client
            .command("DELETE 1")
            .await?
            .contains("Message deleted.")
    );
    modem.quit("EXIT").await?;
    modem.connect("M0THIRD").await?;
    modem.quit("QUIT").await?;

    test.bbs.shutdown().await?;
    modem.closed().await?;
    Ok(())
}

#[tokio::test]
async fn mercury_zmodem_transfers_return_to_commands() -> Result<()> {
    let test = TestBbs::start().await?;
    let contents: Vec<u8> = (0..=255).cycle().take(32_768).collect();
    std::fs::write(test.directory.path().join("files/bulletin.txt"), &contents)?;
    let mut modem = test.accept().await?;
    modem.connect("M0ALICE").await?;
    modem.client.line_ending = b"\r\n";
    modem.client.send_line("DOWNLOAD bulletin.txt").await?;
    modem
        .client
        .read_until(b"Starting ZMODEM download.\r\n")
        .await?;
    receive_zmodem_download(&mut modem.client, &contents).await?;
    assert!(modem.client.command("LIST").await?.contains("No messages."));

    send_zmodem_upload(&mut modem.client, b"uplink.bin", &contents).await?;
    assert!(
        modem
            .client
            .command("INFO")
            .await?
            .contains("BBS callsign: M0BBS")
    );
    assert_eq!(
        std::fs::read(test.directory.path().join("uploads/uplink.bin"))?,
        contents
    );
    let listing = modem.client.command("FILES").await?;
    assert!(listing.contains("bulletin.txt"));
    assert!(!listing.contains("uplink.bin"));
    assert!(
        modem
            .client
            .command("DOWNLOAD ../uplink.bin")
            .await?
            .contains("Invalid file name.")
    );
    modem.quit("QUIT").await?;
    test.bbs.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn mercury_reconnects_after_control_and_data_failure_while_tcp_stays_available() -> Result<()>
{
    let test = TestBbs::start().await?;
    let mut modem = test.accept().await?;
    modem.connect("M0FIRST").await?;
    modem.control.get_mut().shutdown().await?;
    modem.closed().await?;
    let mut tcp = Client::connect(test.bbs.tcp_addr(), "M0TCP", b"\r").await?;
    assert!(tcp.command("INFO").await?.contains("BBS callsign: M0BBS"));
    let mut modem = test.accept().await?;
    modem.connect("M0SECOND").await?;
    modem.client.stream.shutdown().await?;
    modem.closed().await?;
    let mut modem = test.accept().await?;
    modem.connect("M0THIRD").await?;
    assert!(modem.client.command("LIST").await?.contains("No messages."));
    // Shutdown also cancels an active radio session without waiting for EOF.
    test.bbs.shutdown().await?;
    modem.closed().await?;
    Ok(())
}

#[tokio::test]
async fn mercury_setup_failure_is_retried_and_shutdown_interrupts_retry() -> Result<()> {
    let test = TestBbs::start().await?;
    let mut modem = test.sockets().await?;
    modem.expect_command("MYCALL M0BBS").await?;
    modem.event("WRONG").await?;
    modem.closed().await?;
    let mut modem = test.accept().await?;
    modem.connect("M0ALICE").await?;
    modem.control.get_mut().shutdown().await?;
    modem.closed().await?;
    timeout(Duration::from_secs(1), test.bbs.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn mercury_disconnect_without_completion_has_a_deadline() -> Result<()> {
    let test = TestBbs::start().await?;
    let mut modem = test.accept().await?;
    modem.connect("M0ALICE").await?;
    modem.client.send_line("QUIT").await?;
    modem.client.read_until(b"Goodbye.\r\n").await?;
    modem.expect_command("DISCONNECT").await?;
    modem.event("OK").await?;
    // Allow the acknowledgement to be processed, but never send DISCONNECTED.
    sleep(Duration::from_millis(30)).await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    modem.closed().await?;
    timeout(Duration::from_secs(1), test.bbs.shutdown()).await??;
    Ok(())
}

#[tokio::test]
async fn mercury_lost_control_events_reset_the_modem_connection() -> Result<()> {
    let test = TestBbs::start().await?;
    let mut modem = test.sockets().await?;
    modem.expect_command("MYCALL M0BBS").await?;
    // The BBS is still awaiting initialization. Overflow the subscription
    // before allowing setup to complete, so event loss is deterministic.
    modem.event(&"PENDING\rCANCELPENDING\r".repeat(150)).await?;
    modem.event("OK").await?;
    for command in ["PUBLIC OFF", "LISTEN ON"] {
        modem.expect_command(command).await?;
        modem.event("OK").await?;
    }
    modem.closed().await?;
    let mut modem = test.accept().await?;
    modem.connect("M0NEW").await?;
    test.bbs.shutdown().await?;
    modem.closed().await?;
    Ok(())
}

#[tokio::test]
async fn mercury_rejects_invalid_bbs_callsigns_and_can_stop_during_disconnect() -> Result<()> {
    let test = TestBbs::start().await?;
    let mut modem = test.accept().await?;
    // Valid for the modem's 15-character field, but not an ABBS identity.
    modem.event("CONNECTED TOO-LONG-CALL M0BBS 2300").await?;
    modem.expect_command("DISCONNECT").await?;
    modem.event("OK").await?;
    modem.event("DISCONNECTED").await?;
    modem.connect("M0ALICE").await?;
    let logins = modem.client.command("LOGINS").await?;
    assert!(!logins.contains("TOO-LONG-CALL"));
    modem.client.send_line("QUIT").await?;
    modem.client.read_until(b"Goodbye.\r\n").await?;
    modem.expect_command("DISCONNECT").await?;
    modem.event("OK").await?;
    // No DISCONNECTED event: shutdown must not wait for the 60-second deadline.
    timeout(Duration::from_secs(1), test.bbs.shutdown()).await??;
    modem.closed().await?;
    Ok(())
}
