//! Incoming Mercury ARQ sessions on one persistent control/data socket pair.

use std::{
    io,
    pin::Pin,
    task::{Context as TaskContext, Poll},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use log::{info, warn};
use mercury_hf::{ClientConfig, ControlClient, Event, EventReceiver, ListenMode};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::broadcast,
    time::{sleep, timeout},
};

use crate::{
    BbsConfig, Callsign, RadioConfig,
    files::FileArea,
    session::{SessionOptions, SessionRadio, run_session},
    store::{LoginTransport, MailStore},
    terminal::Terminal,
};

const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) async fn supervise(
    config: BbsConfig,
    store: MailStore,
    files: FileArea,
    uploads: FileArea,
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        let result = tokio::select! {
            _ = shutdown.recv() => return,
            result = listen(&config, &store, &files, &uploads) => result,
        };
        if let Err(error) = result {
            warn!("Mercury listener stopped: {error:#}; retrying in five seconds");
        }
        tokio::select! {
            _ = shutdown.recv() => return,
            () = sleep(Duration::from_secs(5)) => {}
        }
    }
}

async fn next_event(events: &mut EventReceiver) -> Result<Event> {
    // Lag means a CONNECTED/DISCONNECTED transition might have been missed.
    // Never try to infer session identity from a possibly newer snapshot.
    match events.recv().await.context("Mercury control events lost")? {
        Event::Closed(error) => Err(error).context("Mercury control connection closed"),
        event => Ok(event),
    }
}

async fn listen(
    config: &BbsConfig,
    store: &MailStore,
    files: &FileArea,
    uploads: &FileArea,
) -> Result<()> {
    let RadioConfig::Mercury {
        host,
        control_port,
        data_port,
    } = &config.radio
    else {
        unreachable!("Mercury listener requires Mercury configuration");
    };
    let modem_config = ClientConfig {
        host: host.clone(),
        control_port: *control_port,
        data_port: *data_port,
        ..ClientConfig::default()
    };
    // Dropping the last ControlClient aborts its driver, including on shutdown
    // or setup failure. A fresh control connection also resets Mercury's data.
    let (client, mut events) = ControlClient::connect(modem_config).await?;
    let mut data = client.open_data_stream().await?;
    client
        .mycall(config.callsign.as_str().parse()?, vec![])
        .await?;
    client.set_public(false).await?;
    client.listen(ListenMode::On).await?;
    info!(
        "listening for Mercury connections to {} on {host}:{control_port}",
        config.callsign
    );

    let mut idle_bytes = [0; 4096];
    loop {
        tokio::select! {
            // Process queued connection transitions before touching payload.
            biased;
            event = next_event(&mut events) => {
                if let Event::Connected(connection) = event? {
                    let remote = match Callsign::parse(connection.source.as_str()) {
                        Ok(remote) => remote,
                        Err(error) => {
                            warn!("rejecting Mercury caller {}: {error:#}", connection.source);
                            disconnect(&client, &mut events, &mut data).await?;
                            continue;
                        }
                    };
                    let _login = RadioSessionLog(remote.clone());
                    let locally_finished = serve(
                        &mut data, &mut events, remote, config, store, files, uploads,
                    ).await?;
                    if locally_finished {
                        disconnect(&client, &mut events, &mut data).await?;
                    }
                }
            }
            read = data.read(&mut idle_bytes) => {
                ensure!(read? != 0, "Mercury data connection closed while idle");
                // Bytes outside a radio session do not belong to a new caller.
            }
        }
    }
}

struct RadioSessionLog(Callsign);

impl Drop for RadioSessionLog {
    fn drop(&mut self) {
        info!("Mercury client {} disconnected", self.0);
    }
}

async fn serve(
    data: &mut TcpStream,
    events: &mut EventReceiver,
    remote: Callsign,
    config: &BbsConfig,
    store: &MailStore,
    files: &FileArea,
    uploads: &FileArea,
) -> Result<bool> {
    let session = async {
        store
            .record_login(remote.clone(), LoginTransport::Mercury)
            .await?;
        info!("Mercury client {remote} logged in");
        run_session(
            Terminal::new(SessionStream(data)),
            remote,
            config.callsign.clone(),
            store.clone(),
            files.clone(),
            uploads.clone(),
            SessionOptions {
                prompt: config.prompt.clone(),
                body_prompt: config.body_prompt.clone(),
                show_bbs_welcome: true,
                radio: SessionRadio::Mercury,
            },
        )
        .await
    };
    tokio::pin!(session);
    loop {
        tokio::select! {
            biased;
            event = next_event(events) => match event? {
                Event::Disconnected => return Ok(false),
                Event::Connected(_) => bail!("unexpected overlapping Mercury connection"),
                _ => {}
            },
            result = &mut session => {
                result?;
                return Ok(true);
            }
        }
    }
}

async fn disconnect(
    client: &ControlClient,
    events: &mut EventReceiver,
    data: &mut TcpStream,
) -> Result<()> {
    timeout(DISCONNECT_TIMEOUT, async {
        // TCP flush is not a radio-delivery acknowledgement. In particular an
        // old BUFFER 0 event cannot prove that our farewell has been delivered.
        client.disconnect_arq().await?;
        let mut discarded = [0; 4096];
        loop {
            tokio::select! {
                biased;
                event = next_event(events) => match event? {
                    Event::Disconnected => return Ok(()),
                    Event::Connected(_) => bail!("Mercury connected before disconnect completed"),
                    _ => {}
                },
                read = data.read(&mut discarded) => {
                    ensure!(read? != 0, "Mercury data connection closed during disconnect");
                }
            }
        }
    })
    .await
    .context("Mercury radio disconnect timed out")?
}

/// BBS shutdown finishes a *radio session*, not the shared modem data socket.
/// Physical EOF is an error so that the supervisor resets both sockets.
struct SessionStream<'a>(&'a mut TcpStream);

impl AsyncRead for SessionStream<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let has_space = buf.remaining() != 0;
        match Pin::new(&mut *self.0).poll_read(cx, buf) {
            Poll::Ready(Ok(())) if has_space && buf.filled().len() == before => {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Mercury data connection closed",
                )))
            }
            result => result,
        }
    }
}

impl AsyncWrite for SessionStream<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}
