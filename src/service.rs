use std::{future::Future, net::SocketAddr, pin::Pin, time::Duration};

use agw::{Port, r#async::AGW};
use anyhow::{Context, Result};
use futures_util::{StreamExt, stream::FuturesUnordered};
use log::{error, info, warn};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
    task::{JoinHandle, JoinSet},
    time::sleep,
};

use crate::{
    callsign::Callsign,
    files::FileArea,
    session::{SessionOptions, run_session},
    store::{LoginTransport, MailStore},
    terminal::Terminal,
};

type Ax25SessionFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

#[derive(Clone, Debug)]
pub struct BbsConfig {
    pub callsign: Callsign,
    pub database_path: std::path::PathBuf,
    pub files_dir: std::path::PathBuf,
    pub uploads_dir: Option<std::path::PathBuf>,
    pub prompt: String,
    pub body_prompt: String,
    pub tcp_listen: SocketAddr,
    pub agw_addr: String,
    pub agw_port: u8,
}

pub struct BbsHandle {
    tcp_addr: SocketAddr,
    shutdown: broadcast::Sender<()>,
    tasks: Vec<JoinHandle<()>>,
}

impl BbsHandle {
    #[must_use]
    pub fn tcp_addr(&self) -> SocketAddr {
        self.tcp_addr
    }

    /// Stop listeners and wait for their background tasks to exit.
    ///
    /// # Errors
    ///
    /// Returns an error if a background task fails or cannot be joined.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(());
        for task in self.tasks {
            task.await.context("BBS background task failed")?;
        }
        Ok(())
    }
}

/// Initialize storage and start the TCP and AGW listeners.
///
/// # Errors
///
/// Returns an error if SQLite initialization or TCP listener binding fails.
pub async fn start(config: BbsConfig) -> Result<BbsHandle> {
    let store = MailStore::open(config.database_path.clone()).await?;
    let files = FileArea::open(config.files_dir.clone()).await?;
    let uploads = match config.uploads_dir.clone() {
        Some(path) => FileArea::open(path).await?,
        None => files.clone(),
    };
    let tcp_listener = TcpListener::bind(config.tcp_listen)
        .await
        .with_context(|| format!("failed to bind TCP listener at {}", config.tcp_listen))?;
    let tcp_addr = tcp_listener.local_addr()?;
    let (shutdown, _) = broadcast::channel(1);

    let tcp_task = tokio::spawn(tcp_listener_loop(
        tcp_listener,
        config.clone(),
        store.clone(),
        files.clone(),
        uploads.clone(),
        shutdown.subscribe(),
    ));
    let agw_task = tokio::spawn(agw_supervisor(
        config,
        store,
        files,
        uploads,
        shutdown.subscribe(),
    ));

    Ok(BbsHandle {
        tcp_addr,
        shutdown,
        tasks: vec![tcp_task, agw_task],
    })
}

async fn tcp_listener_loop(
    listener: TcpListener,
    config: BbsConfig,
    store: MailStore,
    files: FileArea,
    uploads: FileArea,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let config = config.clone();
                    let store = store.clone();
                    let files = files.clone();
                    let uploads = uploads.clone();
                    sessions.spawn(async move {
                        if let Err(error) = Box::pin(handle_tcp_session(stream, config, store, files, uploads)).await {
                            warn!("TCP session ended with error: {error:#}");
                        }
                    });
                }
                Err(error) => error!("TCP accept failed: {error}"),
            },
            Some(result) = sessions.join_next(), if !sessions.is_empty() => {
                if let Err(error) = result {
                    warn!("TCP session task failed: {error}");
                }
            }
        }
    }

    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
}

async fn handle_tcp_session(
    stream: TcpStream,
    config: BbsConfig,
    store: MailStore,
    files: FileArea,
    uploads: FileArea,
) -> Result<()> {
    let mut terminal = Terminal::new(stream);
    terminal
        .write_line(&format!(
            "Welcome to {} amateur radio BBS.",
            config.callsign
        ))
        .await?;

    for attempt in 0..3 {
        terminal.write("Callsign: ").await?;
        let Some(input) = terminal.read_line().await? else {
            return Ok(());
        };
        match Callsign::parse(&input) {
            Ok(callsign) => {
                store
                    .record_login(callsign.clone(), LoginTransport::Tcp)
                    .await?;
                return Box::pin(run_session(
                    terminal,
                    callsign,
                    config.callsign,
                    store,
                    files,
                    uploads,
                    SessionOptions {
                        prompt: config.prompt,
                        body_prompt: config.body_prompt,
                        show_bbs_welcome: false,
                    },
                ))
                .await;
            }
            Err(error) => {
                terminal
                    .write_line(&format!("Invalid callsign: {error}"))
                    .await?;
            }
        }

        if attempt == 2 {
            terminal
                .write_line("Too many invalid login attempts.")
                .await?;
        }
    }
    Ok(())
}

async fn agw_supervisor(
    config: BbsConfig,
    store: MailStore,
    files: FileArea,
    uploads: FileArea,
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        let listener_shutdown = shutdown.resubscribe();
        let result = tokio::select! {
            _ = shutdown.recv() => return,
            result = run_agw_listener(config.clone(), store.clone(), files.clone(), uploads.clone(), listener_shutdown) => result,
        };
        match result {
            Ok(()) => warn!("AGW listener stopped; retrying in five seconds"),
            Err(error) => warn!("AGW listener stopped: {error:#}; retrying in five seconds"),
        }
        tokio::select! {
            _ = shutdown.recv() => return,
            () = sleep(Duration::from_secs(5)) => {}
        }
    }
}

async fn run_agw_listener(
    config: BbsConfig,
    store: MailStore,
    files: FileArea,
    uploads: FileArea,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let agw = AGW::new(&config.agw_addr)
        .await
        .with_context(|| format!("failed to connect to AGW at {}", config.agw_addr))?;
    let bbs_call = config.callsign.to_agw_call()?;
    let mut listener = agw
        .listen(Port(config.agw_port), &bbs_call)
        .await
        .context("failed to listen for AX.25 connections")?;
    info!(
        "listening for AX.25 connections to {} on AGW {} port {}",
        config.callsign, config.agw_addr, config.agw_port
    );

    let mut sessions: FuturesUnordered<Ax25SessionFuture<'_>> = FuturesUnordered::new();

    loop {
        tokio::select! {
            _ = shutdown.recv() => return Ok(()),
            accepted = listener.accept() => {
                let connection = accepted.context("failed to accept AX.25 connection")?;
                let remote = Callsign::parse(&connection.dst().to_string())
                    .context("AGW supplied an invalid remote callsign")?;
                let bbs_callsign = config.callsign.clone();
                let prompt = config.prompt.clone();
                let body_prompt = config.body_prompt.clone();
                let store = store.clone();
                let files = files.clone();
                let uploads = uploads.clone();
                sessions.push(Box::pin(async move {
                    if let Err(error) = store.record_login(remote.clone(), LoginTransport::Ax25).await {
                        warn!("failed to record AX.25 login: {error:#}");
                        return;
                    }
                    if let Err(error) = Box::pin(run_session(
                        Terminal::new(connection),
                        remote,
                        bbs_callsign,
                        store,
                        files,
                        uploads,
                        SessionOptions {
                            prompt,
                            body_prompt,
                            show_bbs_welcome: true,
                        },
                    )).await {
                        warn!("AX.25 session ended with error: {error:#}");
                    }
                }));
            }
            Some(()) = sessions.next(), if !sessions.is_empty() => {}
        }
    }
}
