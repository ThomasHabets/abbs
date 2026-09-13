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

use crate::{callsign::Callsign, session::run_session, store::MailStore, terminal::Terminal};

#[derive(Clone, Debug)]
pub struct BbsConfig {
    pub callsign: Callsign,
    pub database_path: std::path::PathBuf,
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
    pub fn tcp_addr(&self) -> SocketAddr {
        self.tcp_addr
    }

    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(());
        for task in self.tasks {
            task.await.context("BBS background task failed")?;
        }
        Ok(())
    }
}

pub async fn start(config: BbsConfig) -> Result<BbsHandle> {
    let store = MailStore::open(config.database_path.clone()).await?;
    let tcp_listener = TcpListener::bind(config.tcp_listen)
        .await
        .with_context(|| format!("failed to bind TCP listener at {}", config.tcp_listen))?;
    let tcp_addr = tcp_listener.local_addr()?;
    let (shutdown, _) = broadcast::channel(1);

    let tcp_task = tokio::spawn(tcp_listener_loop(
        tcp_listener,
        config.clone(),
        store.clone(),
        shutdown.subscribe(),
    ));
    let agw_task = tokio::spawn(agw_supervisor(config, store, shutdown.subscribe()));

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
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    let config = config.clone();
                    let store = store.clone();
                    sessions.spawn(async move {
                        if let Err(error) = handle_tcp_session(stream, config, store).await {
                            warn!("TCP session from {peer} ended with error: {error:#}");
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

async fn handle_tcp_session(stream: TcpStream, config: BbsConfig, store: MailStore) -> Result<()> {
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
                return run_session(terminal, callsign, config.callsign, store, false).await;
            }
            Err(error) => {
                terminal
                    .write_line(&format!("Invalid callsign: {error}"))
                    .await?
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
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        let listener_shutdown = shutdown.resubscribe();
        let result = tokio::select! {
            _ = shutdown.recv() => return,
            result = run_agw_listener(config.clone(), store.clone(), listener_shutdown) => result,
        };
        match result {
            Ok(()) => warn!("AGW listener stopped; retrying in five seconds"),
            Err(error) => warn!("AGW listener stopped: {error:#}; retrying in five seconds"),
        }
        tokio::select! {
            _ = shutdown.recv() => return,
            _ = sleep(Duration::from_secs(5)) => {}
        }
    }
}

async fn run_agw_listener(
    config: BbsConfig,
    store: MailStore,
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

    type SessionFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    let mut sessions: FuturesUnordered<SessionFuture<'_>> = FuturesUnordered::new();

    loop {
        tokio::select! {
            _ = shutdown.recv() => return Ok(()),
            accepted = listener.accept() => {
                let connection = accepted.context("failed to accept AX.25 connection")?;
                let remote = Callsign::parse(&connection.dst().to_string())
                    .context("AGW supplied an invalid remote callsign")?;
                let bbs_callsign = config.callsign.clone();
                let store = store.clone();
                sessions.push(Box::pin(async move {
                    if let Err(error) = run_session(Terminal::new(connection), remote, bbs_callsign, store, true).await {
                        warn!("AX.25 session ended with error: {error:#}");
                    }
                }));
            }
            Some(()) = sessions.next(), if !sessions.is_empty() => {}
        }
    }
}
