use mm2_engine::{api::Api, auth::Auth, config::Config, runtime, sources, tls};
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
    if let Err(error) = tls::initialize() {
        tracing::error!(error_class=%error,"engine stopped");
        std::process::exit(1);
    }
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
    let mut config_path = PathBuf::from("/app/config.toml");
    let mut startup_mode = None;
    let mut check_only = false;
    let mut arguments = args.iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => config_path = PathBuf::from(arguments.next().ok_or("--config requires a path")?),
            "--mode" => startup_mode = Some(arguments.next().ok_or("--mode requires simulation, trading, both or shadow")?.clone()),
            "--check" => check_only = true,
            _ => return Err("usage: mm2-engine --config <path> [--mode simulation|trading|both|shadow|emulator] [--check]".into()),
        }
    }
    let mut config = Config::load(&config_path)?;
    if let Some(mode) = startup_mode {
        config.mode = mode;
        config.validate()?;
    }
    if check_only {
        tracing::info!(mode=%config.mode,"configuration parsed; credentials, venue and selected host are not verified");
        return Ok(());
    }
    let auth = Auth::from_env()?;
    let start = Instant::now();
    let (source_feed_tx, source_feed_rx) =
        mpsc::channel::<mm2_engine::domain::FeedEvent>(config.feed_capacity);
    let (feed_tx, feed_rx) = mpsc::channel(config.feed_capacity);
    let (fault_tx, fault_rx) = watch::channel(None);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (mut handle, mut actor) =
        runtime::prepare(&config, start, feed_rx, fault_rx, shutdown_rx.clone())?;
    let mut paper_task = None;
    let mut paper_feed = None;
    if config.mode == "both" {
        let mut paper_config = config.clone();
        paper_config.mode = "simulation".into();
        paper_config.live = None;
        paper_config.live_state_dir = None;
        paper_config.state_dir = config.state_dir.join("paper");
        let (paper_tx, paper_rx) = mpsc::channel(config.feed_capacity);
        let (paper_fault_tx, paper_fault_rx) = watch::channel(None);
        let (paper_handle, paper_actor) = runtime::prepare(
            &paper_config,
            start,
            paper_rx,
            paper_fault_rx,
            shutdown_rx.clone(),
        )?;
        handle.attach_paper(paper_handle.clone(), paper_fault_tx.clone());
        actor.attach_paper(paper_handle, paper_fault_tx.clone());
        paper_feed = Some((paper_tx, paper_fault_tx));
        paper_task = Some(tokio::spawn(paper_actor.run()));
    }
    if let Some(settings) = &config.live {
        let live = mm2_live::LiveRuntime::start(
            settings.clone(),
            config
                .live_state_dir
                .clone()
                .ok_or("missing live account ledger location")?,
        )
        .await?;
        actor.attach_live(live);
    }
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|_| "cannot bind HTTP listener")?;
    let mut actor_task = tokio::spawn(actor.run());
    let mut relay_shutdown = shutdown_rx.clone();
    let mut relay_task = tokio::spawn(async move {
        let mut input = source_feed_rx;
        loop {
            tokio::select! {
                biased;
                _=relay_shutdown.changed()=>break,
                event=input.recv()=>match event {
                    Some(event)=>{
                        // Only the primary execution lane can backpressure the source.
                        if let Some((sender,fault))=&paper_feed
                            && sender.try_send(event.clone()).is_err() {
                                fault.send_replace(Some("parallel simulation lost a feed event; rebuild its source state".into()));
                        }
                        if feed_tx.send(event).await.is_err() { break; }
                    },
                    None=>break,
                }
            }
        }
    });
    let source_handle = handle.clone();
    let source_fault = fault_tx.clone();
    let source_shutdown = shutdown_rx.clone();
    let mut source_task = tokio::spawn(async move {
        let result = sources::run(
            config.feed_source,
            start,
            source_handle,
            source_feed_tx,
            source_fault.clone(),
            source_shutdown.clone(),
        )
        .await;
        record_source_exit(&source_fault, &result, *source_shutdown.borrow());
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
    tracing::info!("mm2 engine started paused; execution profile selected at startup");
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
    let drained = tokio::time::timeout(Duration::from_secs(25), async {
        if !actor_done {
            let _ = (&mut actor_task).await;
        }
        if !source_done {
            let _ = (&mut source_task).await;
        }
        if !server_done {
            let _ = (&mut server_task).await;
        }
        let _ = (&mut relay_task).await;
        if let Some(paper) = &mut paper_task {
            let _ = paper.await;
        }
    })
    .await;
    if drained.is_err() {
        actor_task.abort();
        source_task.abort();
        server_task.abort();
        relay_task.abort();
        if let Some(paper) = paper_task {
            paper.abort();
        }
        return Err("shutdown deadline exceeded".into());
    }
    tracing::info!("mm2 engine stopped; acknowledged actions are durable");
    result
}
fn record_source_exit(
    fault: &watch::Sender<Option<String>>,
    result: &Result<(), String>,
    stopping: bool,
) {
    if result.is_err() && !stopping {
        fault.send_if_modified(|first| {
            if first.is_some() {
                false
            } else {
                *first = Some("public/demo source owner stopped".into());
                true
            }
        });
    }
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

#[cfg(test)]
mod source_exit_tests {
    use super::*;

    #[test]
    fn source_exit_preserves_first_fault_instead_of_replacing_it_with_generic_owner_stop() {
        let (fault, observed) = watch::channel(Some("feed queue overflow".to_owned()));
        record_source_exit(&fault, &Err("feed queue overflow".into()), false);
        assert_eq!(observed.borrow().as_deref(), Some("feed queue overflow"));
    }

    #[test]
    fn source_exit_fallback_only_applies_to_unexpected_unclassified_failure() {
        for (result, stopping) in [(Ok(()), false), (Err("shutdown".into()), true)] {
            let (fault, observed) = watch::channel(None);
            record_source_exit(&fault, &result, stopping);
            assert!(observed.borrow().is_none());
        }
        let (fault, observed) = watch::channel(None);
        record_source_exit(&fault, &Err("source task failed".into()), false);
        assert_eq!(
            observed.borrow().as_deref(),
            Some("public/demo source owner stopped")
        );
    }
}
