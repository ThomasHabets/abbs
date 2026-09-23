// Shared terminal and ZMODEM client for TCP and Mercury integration tests.
#![allow(dead_code)]

use anyhow::{Context, Result, bail};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use zmodem2::{Action, Event, FileInfo, Position, Receiver, Sender};

pub struct Client {
    pub stream: TcpStream,
    pub received: Vec<u8>,
    pub line_ending: &'static [u8],
    pub prompt: Vec<u8>,
    pub body_prompt: Vec<u8>,
}

impl Client {
    pub fn from_stream(stream: TcpStream, line_ending: &'static [u8]) -> Self {
        Self {
            stream,
            received: Vec::new(),
            line_ending,
            prompt: b"> ".to_vec(),
            body_prompt: b"> ".to_vec(),
        }
    }

    pub async fn connect(
        address: SocketAddr,
        callsign: &str,
        line_ending: &'static [u8],
    ) -> Result<Self> {
        Self::connect_with_prompt(address, callsign, line_ending, b"> ").await
    }

    pub async fn connect_with_prompt(
        address: SocketAddr,
        callsign: &str,
        line_ending: &'static [u8],
        prompt: &[u8],
    ) -> Result<Self> {
        Self::connect_with_prompts(address, callsign, line_ending, prompt, b"> ").await
    }

    pub async fn connect_with_prompts(
        address: SocketAddr,
        callsign: &str,
        line_ending: &'static [u8],
        prompt: &[u8],
        body_prompt: &[u8],
    ) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let mut client = Self {
            stream,
            received: Vec::new(),
            line_ending,
            prompt: prompt.to_vec(),
            body_prompt: body_prompt.to_vec(),
        };
        client.read_until(b"Callsign: ").await?;
        client.send_line(callsign).await?;
        let greeting = client.read_until(prompt).await?;
        assert!(greeting.contains(&format!("Welcome, {}.", callsign.to_ascii_uppercase())));
        Ok(client)
    }

    pub async fn send_line(&mut self, line: &str) -> Result<()> {
        self.stream.write_all(line.as_bytes()).await?;
        self.stream.write_all(self.line_ending).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn command(&mut self, command: &str) -> Result<String> {
        self.send_line(command).await?;
        let prompt = self.prompt.clone();
        self.read_until(&prompt).await
    }

    pub async fn send_message(
        &mut self,
        recipient: &str,
        subject: &str,
        body: &[&str],
    ) -> Result<String> {
        self.send_line(&format!("SEND {recipient}")).await?;
        self.read_until(b"Subject: ").await?;
        self.send_line(subject).await?;
        let body_prompt = self.body_prompt.clone();
        self.read_until(&body_prompt).await?;
        for line in body {
            self.send_line(line).await?;
            self.read_until(&body_prompt).await?;
        }
        self.send_line(".").await?;
        let prompt = self.prompt.clone();
        self.read_until(&prompt).await
    }

    pub async fn read_until(&mut self, marker: &[u8]) -> Result<String> {
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

pub async fn receive_zmodem_download(client: &mut Client, expected: &[u8]) -> Result<()> {
    let mut receiver = Receiver::new().context("failed to initialize ZMODEM receiver")?;
    let mut received_file = Vec::new();
    let mut session_completed = false;

    loop {
        match receiver.poll() {
            Action::WriteWire(bytes) => {
                let bytes = bytes.to_vec();
                client.stream.write_all(&bytes).await?;
                client.stream.flush().await?;
                receiver.wire_written(bytes.len());
            }
            Action::WriteFile(bytes) => {
                let bytes = bytes.to_vec();
                received_file.extend_from_slice(&bytes);
                receiver
                    .file_written(bytes.len())
                    .context("failed to acknowledge received ZMODEM file data")?;
            }
            Action::Event(Event::FileStarted(info)) => {
                assert_eq!(info.name, b"bulletin.txt");
            }
            Action::Event(Event::SessionCompleted) => session_completed = true,
            Action::Event(Event::Aborted) => bail!("BBS aborted ZMODEM download"),
            Action::Event(_) => {}
            Action::Idle if session_completed => break,
            Action::Idle => {
                if !client.received.is_empty() {
                    let consumed = receiver
                        .submit_wire(&client.received)
                        .context("BBS sent invalid ZMODEM data")?;
                    if consumed != 0 {
                        client.received.drain(..consumed);
                        continue;
                    }
                }

                let mut buffer = [0_u8; 1024];
                let read = timeout(Duration::from_secs(2), client.stream.read(&mut buffer))
                    .await
                    .context("timed out waiting for ZMODEM data")??;
                anyhow::ensure!(read != 0, "BBS disconnected during ZMODEM download");
                client.received.extend_from_slice(&buffer[..read]);
            }
            Action::ReadFile { .. } => bail!("ZMODEM receiver requested source file data"),
            _ => bail!("ZMODEM receiver returned an unsupported action"),
        }
    }

    loop {
        if let Some(end) = find_subsequence(&client.received, b"OO") {
            client.received.drain(..end + 2);
            break;
        }
        let mut buffer = [0_u8; 32];
        let read = timeout(Duration::from_secs(2), client.stream.read(&mut buffer))
            .await
            .context("timed out waiting for ZMODEM final acknowledgement")??;
        anyhow::ensure!(
            read != 0,
            "BBS disconnected before ZMODEM final acknowledgement"
        );
        client.received.extend_from_slice(&buffer[..read]);
    }

    assert_eq!(received_file, expected);
    Ok(())
}

pub async fn send_zmodem_upload(client: &mut Client, name: &[u8], contents: &[u8]) -> Result<()> {
    let size = u32::try_from(contents.len()).context("upload test file is too large")?;
    let mut sender = Sender::new().context("failed to initialize ZMODEM sender")?;
    sender
        .start_file(FileInfo::new(name, Some(Position::new(size))))
        .context("failed to start ZMODEM upload")?;
    let mut session_completed = false;

    loop {
        match sender.poll() {
            Action::WriteWire(bytes) => {
                let bytes = bytes.to_vec();
                client.stream.write_all(&bytes).await?;
                client.stream.flush().await?;
                sender.wire_written(bytes.len());
            }
            Action::ReadFile { offset, max_len } => {
                let offset = usize::try_from(offset.get())?;
                let remaining = contents
                    .get(offset..)
                    .context("ZMODEM sender requested data beyond the test file")?;
                let end = remaining.len().min(max_len);
                anyhow::ensure!(end != 0, "ZMODEM sender requested empty source data");
                sender
                    .submit_file(&remaining[..end])
                    .context("failed to provide ZMODEM upload data")?;
            }
            Action::Event(Event::FileCompleted) => {
                sender.finish().context("failed to finish ZMODEM upload")?;
            }
            Action::Event(Event::SessionCompleted) => session_completed = true,
            Action::Event(Event::Aborted) => bail!("BBS aborted ZMODEM upload"),
            Action::Idle if session_completed => break,
            Action::Idle => {
                if !client.received.is_empty() {
                    let consumed = sender
                        .submit_wire(&client.received)
                        .context("BBS sent invalid ZMODEM data")?;
                    if consumed != 0 {
                        client.received.drain(..consumed);
                        continue;
                    }
                }

                let mut buffer = [0_u8; 1024];
                let read = timeout(Duration::from_secs(2), client.stream.read(&mut buffer))
                    .await
                    .context("timed out waiting for ZMODEM data")??;
                anyhow::ensure!(read != 0, "BBS disconnected during ZMODEM upload");
                client.received.extend_from_slice(&buffer[..read]);
            }
            Action::WriteFile(_) => bail!("ZMODEM sender unexpectedly requested file output"),
            _ => bail!("ZMODEM sender returned an unsupported action"),
        }
    }

    Ok(())
}
