use std::{collections::HashSet, io::SeekFrom, path::Path, str::SplitWhitespace, sync::Arc};

use agw::{Call, Pid, r#async::AGW};
use anyhow::{Context, Result, bail};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, watch},
    time::{Duration, Instant, timeout},
};
use zmodem2::{Action, Event, FileInfo, Position, Receiver, Sender};

use crate::{
    callsign::Callsign,
    files::FileArea,
    store::{LoginTransport, MailStore, Message},
    terminal::{Terminal, TerminalInput},
};

const MAX_SUBJECT_CHARS: usize = 80;
const MAX_BODY_CHARS: usize = 4_000;
const RECENT_LOGIN_LIMIT: usize = 10;
const ZMODEM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const ZMODEM_RETRY_INTERVAL: Duration = Duration::from_secs(10);
const MAX_UPLOAD_BYTES: u32 = 256 * 1024 * 1024;

/// A live AGW endpoint and the dynamically registered source callsigns it
/// owns. The cache is scoped to one AGW TCP connection and is discarded when
/// the supervisor reconnects.
pub(crate) struct AgwEndpoint {
    agw: Arc<AGW>,
    port: agw::Port,
    via: Call,
    connect_via: bool,
    registered: Mutex<HashSet<Call>>,
}

impl AgwEndpoint {
    #[must_use]
    pub(crate) fn new(agw: Arc<AGW>, port: agw::Port, via: Call, connect_via: bool) -> Self {
        Self {
            agw,
            port,
            via,
            connect_via,
            registered: Mutex::new(HashSet::new()),
        }
    }

    async fn connect(
        &self,
        source: &Call,
        destination: &Call,
    ) -> Result<agw::r#async::Connection<'_>> {
        let mut registered = self.registered.lock().await;
        if registered.insert(source.clone())
            && let Err(error) = self.agw.register_callsign(self.port, source).await
        {
            registered.remove(source);
            return Err(error.into());
        }
        drop(registered);

        let connection = if self.connect_via {
            let via = [agw::ViaHop::seen(self.via.clone())];
            self.agw
                .connect_via(self.port, Pid(0xf0), source, destination, &via)
                .await
        } else {
            self.agw
                .connect(self.port, Pid(0xf0), source, destination, &[])
                .await
        };
        connection.context("outgoing AX.25 connection failed")
    }
}

/// Access to the currently connected AGW endpoint.
#[derive(Clone)]
pub(crate) struct OutboundConnector {
    endpoint: watch::Receiver<Option<Arc<AgwEndpoint>>>,
}

impl OutboundConnector {
    #[must_use]
    pub(crate) fn new(endpoint: watch::Receiver<Option<Arc<AgwEndpoint>>>) -> Self {
        Self { endpoint }
    }

    fn active(&self) -> Result<Arc<AgwEndpoint>> {
        self.endpoint
            .borrow()
            .clone()
            .context("AGW is currently unavailable")
    }
}

pub(crate) struct SessionOptions {
    pub prompt: String,
    pub body_prompt: String,
    pub show_bbs_welcome: bool,
    pub outbound: Option<OutboundConnector>,
}

pub async fn run_session<S>(
    mut terminal: Terminal<S>,
    identity: Callsign,
    bbs_callsign: Callsign,
    store: MailStore,
    files: FileArea,
    uploads: FileArea,
    options: SessionOptions,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if options.show_bbs_welcome {
        terminal
            .write_line(&format!("Welcome to {bbs_callsign} amateur radio BBS."))
            .await?;
    }
    terminal
        .write_line(&format!("Welcome, {identity}."))
        .await?;
    terminal.write_line("Type HELP for commands.").await?;

    // ZMODEM's final acknowledgement is followed very closely by the sender
    // exiting.  Do not put ordinary terminal bytes behind it: a client-side
    // `rz` can still be consuming the final frame and would treat a prompt as
    // protocol input.  After a successful download, wait for the caller's
    // next command before resuming normal prompt output.
    let mut write_prompt = true;
    loop {
        if write_prompt {
            terminal.write(&options.prompt).await?;
        }
        write_prompt = true;
        let Some(input) = terminal.read_input().await? else {
            terminal.shutdown().await?;
            return Ok(());
        };
        let line = match input {
            TerminalInput::Line(line) => line,
            TerminalInput::Zmodem(initial) => {
                write_prompt = handle_zmodem_input(&mut terminal, &uploads, initial).await;
                continue;
            }
        };

        let mut fields = line.split_whitespace();
        let Some(command) = fields.next() else {
            continue;
        };
        let command = command.to_ascii_uppercase();

        match command.as_str() {
            "HELP" if fields.next().is_none() => write_help(&mut terminal).await?,
            "INFO" if fields.next().is_none() => write_info(&mut terminal, &bbs_callsign).await?,
            "LIST" if fields.next().is_none() => {
                list_messages(&mut terminal, &store, identity.clone()).await?;
            }
            "SENT" if fields.next().is_none() => {
                list_sent_messages(&mut terminal, &store, identity.clone()).await?;
            }
            "LOGINS" if fields.next().is_none() => {
                list_recent_logins(&mut terminal, &store).await?;
            }
            "HEARD" if fields.next().is_none() => {
                list_recent_ax25_logins(&mut terminal, &store).await?;
            }
            "FILES" if fields.next().is_none() => {
                list_files(&mut terminal, &files).await?;
            }
            "DOWNLOAD" => {
                write_prompt = download_command(&mut terminal, &files, &mut fields).await?;
            }
            "READ" => read_command(&mut terminal, &store, &identity, &mut fields).await?,
            "DELETE" => delete_command(&mut terminal, &store, &identity, &mut fields).await?,
            "CONNECT" => {
                if matches!(
                    connect_command(
                        &mut terminal,
                        &identity,
                        &mut fields,
                        options.outbound.as_ref(),
                    )
                    .await?,
                    ConnectExit::ClientDisconnected
                ) {
                    terminal.shutdown().await?;
                    return Ok(());
                }
            }
            "SEND" => {
                send_command(
                    &mut terminal,
                    &store,
                    &identity,
                    &mut fields,
                    &options.body_prompt,
                )
                .await?;
            }
            "QUIT" | "BYE" | "EXIT" if fields.next().is_none() => {
                terminal.write_line("Goodbye.").await?;
                terminal.shutdown().await?;
                return Ok(());
            }
            _ => {
                terminal
                    .write_line("Unknown command. Type HELP for commands.")
                    .await?;
            }
        }
    }
}

async fn handle_zmodem_input<S>(
    terminal: &mut Terminal<S>,
    uploads: &FileArea,
    initial: Vec<u8>,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match receive_zmodem(terminal, uploads, initial).await {
        Ok(()) => {
            // A ZMODEM sender answers the receiver's final ZFIN with `OO`.
            // Consume it so it cannot prefix the client's next command.
            let _ = timeout(Duration::from_secs(2), terminal.consume_zmodem_final_ack()).await;
            false
        }
        Err(_) => true,
    }
}

enum ConnectExit {
    Continue,
    ClientDisconnected,
}

async fn connect_command<S>(
    terminal: &mut Terminal<S>,
    identity: &Callsign,
    fields: &mut SplitWhitespace<'_>,
    outbound: Option<&OutboundConnector>,
) -> Result<ConnectExit>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(destination) = fields.next() else {
        terminal
            .write_line("Usage: CONNECT <destination> <ssid>")
            .await?;
        return Ok(ConnectExit::Continue);
    };
    let Some(ssid) = fields.next() else {
        terminal
            .write_line("Usage: CONNECT <destination> <ssid>")
            .await?;
        return Ok(ConnectExit::Continue);
    };
    if fields.next().is_some() {
        terminal
            .write_line("Usage: CONNECT <destination> <ssid>")
            .await?;
        return Ok(ConnectExit::Continue);
    }

    let destination = match Callsign::parse(destination) {
        Ok(destination) => destination,
        Err(error) => {
            terminal
                .write_line(&format!("Invalid destination callsign: {error}"))
                .await?;
            return Ok(ConnectExit::Continue);
        }
    };
    let ssid = match ssid.parse::<u8>() {
        Ok(ssid) if ssid <= 15 => ssid,
        _ => {
            terminal
                .write_line("SSID must be a number from 0 to 15.")
                .await?;
            return Ok(ConnectExit::Continue);
        }
    };
    let source = identity.with_ssid(ssid)?;
    let Some(outbound) = outbound else {
        terminal
            .write_line("CONNECT is disabled for TCP clients.")
            .await?;
        return Ok(ConnectExit::Continue);
    };
    let endpoint = match outbound.active() {
        Ok(endpoint) => endpoint,
        Err(error) => {
            terminal
                .write_line(&format!("Cannot connect: {error}"))
                .await?;
            return Ok(ConnectExit::Continue);
        }
    };
    let source_call = source.to_agw_call()?;
    let destination_call = destination.to_agw_call()?;
    terminal
        .write_line(&format!("Connecting {source} to {destination}..."))
        .await?;
    let mut remote = match endpoint.connect(&source_call, &destination_call).await {
        Ok(remote) => remote,
        Err(error) => {
            terminal
                .write_line(&format!("Connection failed: {error:#}"))
                .await?;
            return Ok(ConnectExit::Continue);
        }
    };

    terminal
        .write_line("Connected. Enter ~. on a line by itself to return here.")
        .await?;
    bridge_remote_bbs(terminal, &mut remote).await
}

async fn bridge_remote_bbs<S>(
    terminal: &mut Terminal<S>,
    remote: &mut agw::r#async::Connection<'_>,
) -> Result<ConnectExit>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut remote_data = [0_u8; 1024];
    loop {
        tokio::select! {
            line = terminal.read_line() => match line? {
                Some(line) if line == "~." => {
                    remote.shutdown().await.context("disconnecting remote BBS failed")?;
                    terminal.write_line("Disconnected from remote BBS.").await?;
                    return Ok(ConnectExit::Continue);
                }
                Some(line) => {
                    let mut output = line.into_bytes();
                    output.push(b'\r');
                    remote.write_all(&output).await?;
                    remote.flush().await?;
                }
                None => {
                    let _ = remote.shutdown().await;
                    return Ok(ConnectExit::ClientDisconnected);
                }
            },
            read = remote.read(&mut remote_data) => match read {
                Ok(0) => {
                    terminal.write_line("Remote BBS disconnected.").await?;
                    return Ok(ConnectExit::Continue);
                }
                Ok(read) => terminal.write_bytes(&remote_data[..read]).await?,
                Err(error) => {
                    terminal.write_line(&format!("Remote BBS error: {error}")).await?;
                    return Ok(ConnectExit::Continue);
                }
            },
        }
    }
}

struct UploadFile {
    final_path: std::path::PathBuf,
    part_path: std::path::PathBuf,
    file: tokio::fs::File,
}

async fn receive_zmodem<S>(
    terminal: &mut Terminal<S>,
    files: &FileArea,
    initial: Vec<u8>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut receiver = Receiver::new().context("failed to initialize ZMODEM receiver")?;
    receiver.set_manual_file_accept(true);
    let mut wire = initial;
    let mut read_buffer = [0_u8; 8 * 1024];
    let mut current: Option<UploadFile> = None;
    let mut completed = false;

    loop {
        match receiver.poll() {
            Action::WriteWire(bytes) => {
                let bytes = bytes.to_vec();
                terminal.write_bytes(&bytes).await?;
                receiver.wire_written(bytes.len());
            }
            Action::WriteFile(bytes) => {
                let bytes = bytes.to_vec();
                let upload = current.as_mut().context("ZMODEM data without a file")?;
                upload.file.write_all(&bytes).await?;
                receiver
                    .file_written(bytes.len())
                    .map_err(anyhow::Error::msg)?;
            }
            Action::Event(Event::FileStarted(info)) => {
                let Ok(name) = std::str::from_utf8(info.name) else {
                    receiver.skip_file().map_err(anyhow::Error::msg)?;
                    continue;
                };
                let size = info.size.map(Position::get);
                let Ok((final_path, part_path)) = files.upload_paths(name) else {
                    receiver.skip_file().map_err(anyhow::Error::msg)?;
                    continue;
                };
                if size.is_none_or(|size| size > MAX_UPLOAD_BYTES)
                    || tokio::fs::try_exists(&final_path).await?
                    || tokio::fs::try_exists(&part_path).await?
                {
                    receiver.skip_file().map_err(anyhow::Error::msg)?;
                    continue;
                }
                let file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&part_path)
                    .await?;
                receiver.accept_file_at(0).map_err(anyhow::Error::msg)?;
                current = Some(UploadFile {
                    final_path,
                    part_path,
                    file,
                });
            }
            Action::Event(Event::FileCompleted) => {
                let upload = current.take().context("ZMODEM completed unknown file")?;
                upload.file.sync_all().await?;
                tokio::fs::rename(&upload.part_path, &upload.final_path).await?;
            }
            Action::Event(Event::SessionCompleted) => completed = true,
            Action::Event(Event::Aborted) => bail!("client aborted ZMODEM upload"),
            Action::Event(_) => {}
            Action::Idle if completed => return Ok(()),
            Action::Idle => {
                if !wire.is_empty() {
                    let consumed = receiver.submit_wire(&wire).map_err(anyhow::Error::msg)?;
                    if consumed != 0 {
                        wire.drain(..consumed);
                        continue;
                    }
                }
                let read = terminal.read_bytes(&mut read_buffer).await?;
                anyhow::ensure!(read != 0, "client disconnected during ZMODEM upload");
                wire.extend_from_slice(&read_buffer[..read]);
            }
            Action::ReadFile { .. } => bail!("ZMODEM receiver requested source data"),
            _ => bail!("unsupported ZMODEM receiver action"),
        }
    }
}

async fn download_command<S>(
    terminal: &mut Terminal<S>,
    files: &FileArea,
    fields: &mut SplitWhitespace<'_>,
) -> Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(name) = fields.next() else {
        terminal.write_line("Usage: DOWNLOAD <file>").await?;
        return Ok(true);
    };
    if fields.next().is_some() {
        terminal.write_line("Usage: DOWNLOAD <file>").await?;
        return Ok(true);
    }
    Box::pin(download_file(terminal, files, name)).await
}

async fn read_command<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    identity: &Callsign,
    fields: &mut SplitWhitespace<'_>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(id) = parse_message_id(terminal, fields, "READ").await? else {
        return Ok(());
    };
    read_message(terminal, store, identity.clone(), id).await
}

async fn delete_command<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    identity: &Callsign,
    fields: &mut SplitWhitespace<'_>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(id) = parse_message_id(terminal, fields, "DELETE").await? else {
        return Ok(());
    };
    delete_message(terminal, store, identity.clone(), id).await
}

async fn parse_message_id<S>(
    terminal: &mut Terminal<S>,
    fields: &mut SplitWhitespace<'_>,
    command: &str,
) -> Result<Option<i64>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(id) = fields.next() else {
        terminal
            .write_line(&format!("Usage: {command} <id>"))
            .await?;
        return Ok(None);
    };
    if fields.next().is_some() {
        terminal
            .write_line(&format!("Usage: {command} <id>"))
            .await?;
        return Ok(None);
    }
    match id.parse::<i64>() {
        Ok(id) if id > 0 => Ok(Some(id)),
        _ => {
            terminal
                .write_line("Message ID must be a positive number.")
                .await?;
            Ok(None)
        }
    }
}

async fn send_command<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    identity: &Callsign,
    fields: &mut SplitWhitespace<'_>,
    prompt: &str,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(recipient) = fields.next() else {
        terminal.write_line("Usage: SEND <callsign|ALL>").await?;
        return Ok(());
    };
    if fields.next().is_some() {
        terminal.write_line("Usage: SEND <callsign|ALL>").await?;
        return Ok(());
    }
    compose_message(terminal, store, identity.clone(), recipient, prompt).await
}

async fn write_help<S>(terminal: &mut Terminal<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    terminal.write_line("Commands:").await?;
    terminal
        .write_line("  LIST                 List public and your private messages")
        .await?;
    terminal
        .write_line("  SENT                 List messages you sent")
        .await?;
    terminal
        .write_line("  LOGINS               List the 10 most recent logins")
        .await?;
    terminal
        .write_line("  HEARD                List the 10 most recent AX.25 stations")
        .await?;
    terminal
        .write_line("  FILES                List files available for download")
        .await?;
    terminal
        .write_line("  DOWNLOAD <file>      Download a file using ZMODEM")
        .await?;
    terminal
        .write_line("  CONNECT <call> <ssid> Connect to a remote BBS over AX.25")
        .await?;
    terminal
        .write_line("  READ <id>            Read a visible message")
        .await?;
    terminal
        .write_line("  DELETE <id>          Delete mail you sent or received")
        .await?;
    terminal
        .write_line("  SEND <callsign|ALL>  Send private mail or post publicly")
        .await?;
    terminal
        .write_line("  INFO                 Show BBS information")
        .await?;
    terminal
        .write_line("  HELP                 Show this help")
        .await?;
    terminal
        .write_line("  QUIT, BYE, EXIT      Disconnect")
        .await?;
    Ok(())
}

async fn write_info<S>(terminal: &mut Terminal<S>, bbs_callsign: &Callsign) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    terminal
        .write_line(&format!("ABBS {}", env!("CARGO_PKG_VERSION")))
        .await?;
    terminal
        .write_line(&format!("BBS callsign: {bbs_callsign}"))
        .await?;
    Ok(())
}

async fn list_files<S>(terminal: &mut Terminal<S>, files: &FileArea) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let available = files.list().await?;
    if available.is_empty() {
        terminal.write_line("No files available.").await?;
        return Ok(());
    }

    terminal.write_line("Available files:").await?;
    for file in available {
        terminal
            .write_line(&format!("{} ({} bytes)", file.name, file.size))
            .await?;
    }
    Ok(())
}

async fn download_file<S>(terminal: &mut Terminal<S>, files: &FileArea, name: &str) -> Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let path = match files.resolve_download(name).await {
        Ok(Some(path)) => path,
        Ok(None) => {
            terminal.write_line("File not found.").await?;
            return Ok(true);
        }
        Err(_) => {
            terminal.write_line("Invalid file name.").await?;
            return Ok(true);
        }
    };

    terminal.write_line("Starting ZMODEM download.").await?;
    if Box::pin(send_zmodem(terminal, &path, name)).await.is_ok() {
        // Leave the wire silent after the sender finishes. The remote `rz` needs
        // to consume the last ZMODEM frame before the client sends its next
        // command, at which point the ordinary command response is safe.
        Ok(false)
    } else {
        terminal.write_line("Download failed.").await?;
        Ok(true)
    }
}

async fn send_zmodem<S>(terminal: &mut Terminal<S>, path: &Path, name: &str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let size = u32::try_from(tokio::fs::metadata(path).await?.len())
        .context("file is too large for ZMODEM")?;
    let mut file = tokio::fs::File::open(path)
        .await
        .context("failed to open download file")?;
    let mut sender = Sender::new().context("failed to initialize ZMODEM sender")?;
    sender
        .start_file(FileInfo::new(name.as_bytes(), Some(Position::new(size))))
        .context("failed to offer file for ZMODEM download")?;
    let mut client_bytes = Vec::new();
    let mut read_buffer = [0_u8; 8 * 1024];
    let mut last_client_data = Instant::now();
    let mut session_completed = false;

    loop {
        match sender.poll() {
            Action::WriteWire(bytes) => {
                let bytes = bytes.to_vec();
                terminal.write_bytes(&bytes).await?;
                sender.wire_written(bytes.len());
            }
            Action::ReadFile { offset, max_len } => {
                file.seek(SeekFrom::Start(u64::from(offset.get()))).await?;
                let mut file_bytes = vec![0_u8; max_len];
                let read = file.read(&mut file_bytes).await?;
                anyhow::ensure!(read != 0, "download file ended unexpectedly");
                sender
                    .submit_file(&file_bytes[..read])
                    .context("failed to submit ZMODEM file data")?;
            }
            Action::Event(Event::FileCompleted) => sender
                .finish()
                .context("failed to finish ZMODEM file transfer")?,
            Action::Event(Event::SessionCompleted) => session_completed = true,
            Action::Event(Event::Aborted) => bail!("client aborted ZMODEM transfer"),
            Action::Event(_) => {}
            Action::Idle if session_completed => return Ok(()),
            Action::Idle => {
                if !client_bytes.is_empty() {
                    let consumed = sender
                        .submit_wire(&client_bytes)
                        .context("invalid ZMODEM data from client")?;
                    if consumed != 0 {
                        client_bytes.drain(..consumed);
                        continue;
                    }
                }

                match timeout(ZMODEM_RETRY_INTERVAL, terminal.read_bytes(&mut read_buffer)).await {
                    Ok(Ok(0)) => bail!("client disconnected during ZMODEM transfer"),
                    Ok(Ok(read)) => {
                        client_bytes.extend_from_slice(&read_buffer[..read]);
                        last_client_data = Instant::now();
                    }
                    Ok(Err(error)) => {
                        return Err(error).context("failed to read client ZMODEM input");
                    }
                    Err(_) if last_client_data.elapsed() >= ZMODEM_IDLE_TIMEOUT => {
                        bail!("ZMODEM transfer timed out")
                    }
                    Err(_) => sender
                        .timeout()
                        .context("failed to retry ZMODEM handshake")?,
                }
            }
            Action::WriteFile(_) => bail!("ZMODEM sender requested file output"),
            _ => bail!("ZMODEM sender returned an unsupported action"),
        }
    }
}

async fn list_recent_logins<S>(terminal: &mut Terminal<S>, store: &MailStore) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let logins = store.recent_logins(RECENT_LOGIN_LIMIT).await?;
    if logins.is_empty() {
        terminal.write_line("No logins recorded.").await?;
        return Ok(());
    }

    terminal.write_line("Recent logins:").await?;
    for login in logins {
        let transport = match login.transport {
            LoginTransport::Tcp => "TCP",
            LoginTransport::Ax25 => "AX.25",
        };
        terminal
            .write_line(&format!(
                "{} via {transport} at {}",
                login.callsign, login.logged_in_at
            ))
            .await?;
    }
    Ok(())
}

async fn list_recent_ax25_logins<S>(terminal: &mut Terminal<S>, store: &MailStore) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let logins = store.recent_ax25_logins(RECENT_LOGIN_LIMIT).await?;
    if logins.is_empty() {
        terminal.write_line("No AX.25 stations heard.").await?;
        return Ok(());
    }

    terminal.write_line("Recent AX.25 stations:").await?;
    for login in logins {
        terminal
            .write_line(&format!("{} at {}", login.callsign, login.logged_in_at))
            .await?;
    }
    Ok(())
}

async fn list_sent_messages<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    sender: Callsign,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let messages = store.list_sent(sender).await?;
    if messages.is_empty() {
        terminal.write_line("No sent messages.").await?;
        return Ok(());
    }

    for message in messages {
        let kind = if message.recipient.is_some() {
            "MAIL"
        } else {
            "PUBLIC"
        };
        let recipient = message
            .recipient
            .as_ref()
            .map_or_else(|| "ALL".to_owned(), ToString::to_string);
        terminal
            .write_line(&format!(
                "#{} [{kind}] TO {recipient} {} - {}",
                message.id, message.created_at, message.subject
            ))
            .await?;
    }
    Ok(())
}

async fn list_messages<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    viewer: Callsign,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let messages = store.list_visible(viewer).await?;
    if messages.is_empty() {
        terminal.write_line("No messages.").await?;
        return Ok(());
    }

    for message in messages {
        let kind = if message.recipient.is_some() {
            "MAIL"
        } else {
            "PUBLIC"
        };
        terminal
            .write_line(&format!(
                "#{} [{kind}] FROM {} {} - {}",
                message.id, message.sender, message.created_at, message.subject
            ))
            .await?;
    }
    Ok(())
}

async fn read_message<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    viewer: Callsign,
    id: i64,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Some(message) = store.read_visible(viewer, id).await? else {
        terminal.write_line("Message not found.").await?;
        return Ok(());
    };
    write_message(terminal, message).await
}

async fn delete_message<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    caller: Callsign,
    id: i64,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if store.delete_authorized(caller, id).await? {
        terminal.write_line("Message deleted.").await?;
    } else {
        terminal
            .write_line("Message not found or cannot be deleted.")
            .await?;
    }
    Ok(())
}

async fn write_message<S>(terminal: &mut Terminal<S>, message: Message) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let recipient = message
        .recipient
        .as_ref()
        .map_or_else(|| "ALL".to_owned(), ToString::to_string);
    terminal
        .write_line(&format!("Message #{}", message.id))
        .await?;
    terminal
        .write_line(&format!("From: {}", message.sender))
        .await?;
    terminal.write_line(&format!("To: {recipient}")).await?;
    terminal
        .write_line(&format!("Date: {}", message.created_at))
        .await?;
    terminal
        .write_line(&format!("Subject: {}", message.subject))
        .await?;
    terminal.write_line("").await?;
    for line in message.body.split('\n') {
        terminal.write_line(line).await?;
    }
    Ok(())
}

async fn compose_message<S>(
    terminal: &mut Terminal<S>,
    store: &MailStore,
    sender: Callsign,
    recipient_input: &str,
    prompt: &str,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let recipient = if recipient_input.eq_ignore_ascii_case("ALL") {
        None
    } else {
        match Callsign::parse(recipient_input) {
            Ok(callsign) => Some(callsign),
            Err(error) => {
                terminal
                    .write_line(&format!("Invalid recipient: {error}"))
                    .await?;
                return Ok(());
            }
        }
    };

    terminal.write("Subject: ").await?;
    let Some(subject_line) = terminal.read_line().await? else {
        return Ok(());
    };
    let subject = subject_line.trim();
    if subject.is_empty() {
        terminal.write_line("Subject cannot be empty.").await?;
        return Ok(());
    }
    if subject.chars().count() > MAX_SUBJECT_CHARS {
        terminal
            .write_line(&format!(
                "Subject cannot exceed {MAX_SUBJECT_CHARS} characters."
            ))
            .await?;
        return Ok(());
    }

    terminal
        .write_line("Enter message text. End with a line containing only a period.")
        .await?;
    let mut lines = Vec::new();
    let mut character_count = 0;
    loop {
        terminal.write(prompt).await?;
        let Some(line) = terminal.read_line().await? else {
            return Ok(());
        };
        if line == "." {
            break;
        }

        character_count += line.chars().count();
        if !lines.is_empty() {
            character_count += 1;
        }
        if character_count > MAX_BODY_CHARS {
            terminal
                .write_line(&format!(
                    "Message cannot exceed {MAX_BODY_CHARS} characters."
                ))
                .await?;
            return Ok(());
        }
        lines.push(line);
    }

    let body = lines.join("\n");
    if body.trim().is_empty() {
        terminal.write_line("Message body cannot be empty.").await?;
        return Ok(());
    }

    let id = store
        .save(sender, recipient, subject.to_owned(), body)
        .await?;
    terminal
        .write_line(&format!("Message #{id} saved."))
        .await?;
    Ok(())
}
