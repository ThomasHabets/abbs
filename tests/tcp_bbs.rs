use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use abbs::{BbsConfig, BbsHandle, Callsign, start};
use anyhow::{Context, Result};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

struct Client {
    stream: TcpStream,
    received: Vec<u8>,
    line_ending: &'static [u8],
}

impl Client {
    async fn connect(
        address: SocketAddr,
        callsign: &str,
        line_ending: &'static [u8],
    ) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let mut client = Self {
            stream,
            received: Vec::new(),
            line_ending,
        };
        client.read_until(b"Callsign: ").await?;
        client.send_line(callsign).await?;
        let greeting = client.read_until(b"> ").await?;
        assert!(greeting.contains(&format!("Welcome, {}.", callsign.to_ascii_uppercase())));
        Ok(client)
    }

    async fn send_line(&mut self, line: &str) -> Result<()> {
        self.stream.write_all(line.as_bytes()).await?;
        self.stream.write_all(self.line_ending).await?;
        self.stream.flush().await?;
        Ok(())
    }

    async fn command(&mut self, command: &str) -> Result<String> {
        self.send_line(command).await?;
        self.read_until(b"> ").await
    }

    async fn send_message(
        &mut self,
        recipient: &str,
        subject: &str,
        body: &[&str],
    ) -> Result<String> {
        self.send_line(&format!("SEND {recipient}")).await?;
        self.read_until(b"Subject: ").await?;
        self.send_line(subject).await?;
        self.read_until(b"> ").await?;
        for line in body {
            self.send_line(line).await?;
            self.read_until(b"> ").await?;
        }
        self.send_line(".").await?;
        self.read_until(b"> ").await
    }

    async fn read_until(&mut self, marker: &[u8]) -> Result<String> {
        loop {
            if let Some(end) = find_subsequence(&self.received, marker) {
                let output = self
                    .received
                    .drain(..end + marker.len())
                    .collect::<Vec<_>>();
                return String::from_utf8(output).context("BBS output was not valid UTF-8");
            }

            let mut buffer = [0_u8; 512];
            let read = timeout(Duration::from_secs(2), self.stream.read(&mut buffer))
                .await
                .context("timed out waiting for BBS output")??;
            anyhow::ensure!(
                read != 0,
                "BBS closed the connection before sending expected output"
            );
            self.received.extend_from_slice(&buffer[..read]);
        }
    }
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "abbs-tcp-test-{}-{}.sqlite3",
        std::process::id(),
        NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
    ))
}

async fn start_test_bbs() -> Result<(BbsHandle, PathBuf)> {
    let database_path = database_path();
    let bbs = start(BbsConfig {
        callsign: Callsign::parse("M0BBS")?,
        database_path: database_path.clone(),
        tcp_listen: "127.0.0.1:0".parse()?,
        // No AGW server is needed for TCP functionality; the BBS must remain
        // available while its radio listener retries.
        agw_addr: "127.0.0.1:9".into(),
        agw_port: 1,
    })
    .await?;
    Ok((bbs, database_path))
}

fn remove_database(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("sqlite3-wal"));
    let _ = fs::remove_file(path.with_extension("sqlite3-shm"));
}

#[tokio::test]
async fn tcp_clients_can_exchange_private_and_public_messages_with_cr_and_crlf() -> Result<()> {
    let (bbs, database_path) = start_test_bbs().await?;
    let address = bbs.tcp_addr();

    let mut alice = Client::connect(address, "m0alice", b"\r").await?;
    let mut bob = Client::connect(address, "m0bob", b"\r\n").await?;
    let mut eve = Client::connect(address, "m0eve", b"\r\n").await?;

    let logins = eve.command("LOGINS").await?;
    assert!(logins.contains("M0ALICE via TCP"));
    assert!(logins.contains("M0BOB via TCP"));
    assert!(logins.contains("M0EVE via TCP"));
    assert!(!logins.contains("127.0.0.1"));

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

    bbs.shutdown().await?;
    remove_database(&database_path);
    Ok(())
}
