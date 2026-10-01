use mm2_engine::{api::Api, auth::Auth, config::Config, runtime, sources};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};

fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .init();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime creation");
    let outcome = runtime.block_on(run());
    runtime.shutdown_timeout(Duration::from_secs(1));
    if let Err(error) = outcome {
        tracing::error!(error_class=%error,"engine stopped");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), String> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.first().is_some_and(|v| v == "healthcheck") {
        let port = if args.len() == 3 && args[1] == "--port" {
            args[2]
                .parse::<u16>()
                .map_err(|_| "invalid healthcheck port")?
        } else if args.len() == 1 {
            8088
        } else {
            return Err("usage: healthcheck [--port 8088]".into());
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|_| "health client failed")?;
        let response = client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
            .map_err(|_| "health transport failed")?;
        if response.status().is_success() {
            return Ok(());
        }
        return Err("health HTTP failed".into());
    }
    if args.len() != 2 || args[0] != "--config" {
        return Err("usage: mm2-engine --config config/demo.toml".into());
    }
    let config = Config::load(&PathBuf::from(&args[1]))?;
    let auth = Auth::from_env()?;
    let start = Instant::now();
    let (feed_tx, feed_rx) = mpsc::channel(config.feed_capacity);
    let (fault_tx, fault_rx) = watch::channel(None);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (handle, actor) = runtime::prepare(&config, start, feed_rx, fault_rx, shutdown_rx.clone())?;
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|_| "cannot bind HTTP listener")?;
    let mut actor_task = tokio::spawn(actor.run());
    let source_handle = handle.clone();
    let source_fault = fault_tx.clone();
    let source_shutdown = shutdown_rx.clone();
    let mut source_task = tokio::spawn(async move {
        let result = sources::run(
            config.feed_source,
            start,
            source_handle,
            feed_tx,
            source_fault.clone(),
            source_shutdown.clone(),
        )
        .await;
        if result.is_err() && !*source_shutdown.borrow() {
            source_fault.send_replace(Some("public/demo source owner stopped".into()));
        }
        result
    });
    let mut server_shutdown = shutdown_rx.clone();
    let mut server_task = tokio::spawn(async move {
        axum::serve(listener, Api::new(handle, auth).router())
            .with_graceful_shutdown(async move {
                while !*server_shutdown.borrow() {
                    if server_shutdown.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await
    });
    tracing::info!("mm2 engine started; live order gateway is deliberately absent");
    let mut actor_done = false;
    let mut source_done = false;
    let mut server_done = false;
    let result: Result<(), String> = tokio::select! {
        _ = signal() => Ok(()),
        _ = &mut actor_task => { actor_done=true; Err("core actor stopped unexpectedly".into()) },
        result = &mut source_task => { source_done=true; match result { Ok(Ok(())) => Err("source owner stopped unexpectedly".into()),Ok(Err(reason)) => Err(reason.chars().take(256).collect()),Err(_) => Err("source owner task failed".into()) } },
        _ = &mut server_task => { server_done=true; Err("HTTP server stopped unexpectedly".into()) },
    };
    shutdown_tx.send_replace(true);
    // The outer deadline also bounds a stalled disk worker; process exit releases its OS fence.
    let drained = tokio::time::timeout(Duration::from_secs(7), async {
        if !actor_done {
            let _ = (&mut actor_task).await;
        }
        if !source_done {
            let _ = (&mut source_task).await;
        }
        if !server_done {
            let _ = (&mut server_task).await;
        }
    })
    .await;
    if drained.is_err() {
        actor_task.abort();
        source_task.abort();
        server_task.abort();
        return Err("shutdown deadline exceeded".into());
    }
    tracing::info!("mm2 engine stopped; acknowledged actions are durable");
    result
}
async fn signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _=tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
