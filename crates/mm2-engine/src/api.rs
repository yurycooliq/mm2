use crate::{
    auth::Auth,
    config::Limits,
    journal::Command,
    runtime::{Failure, Handle, wall_ms},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post, put},
};
use futures_util::stream;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct Api {
    handle: Handle,
    auth: Arc<Auth>,
    sse: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    rate: Arc<Mutex<(Instant, u32)>>,
}
impl Api {
    pub fn new(handle: Handle, auth: Auth) -> Self {
        Self {
            handle,
            auth: Arc::new(auth),
            sse: Arc::new(Semaphore::new(8)),
            requests: Arc::new(Semaphore::new(64)),
            rate: Arc::new(Mutex::new((Instant::now(), 0))),
        }
    }
    pub fn router(self) -> Router {
        Router::new()
            .route("/healthz", get(health))
            .route("/readyz", get(ready))
            .route("/v1/state", get(state))
            .route("/v1/status", get(state))
            .route("/v1/events", get(events))
            .route("/v1/control", post(control))
            .route("/v1/limits", put(limits))
            .route("/v1/auth/telegram", post(telegram))
            .layer(DefaultBodyLimit::max(16_384))
            .layer(middleware::from_fn_with_state(
                self.clone(),
                bounded_requests,
            ))
            .with_state(self)
    }
}

async fn bounded_requests(State(api): State<Api>, request: Request, next: Next) -> Response {
    let Ok(_permit) = api.requests.clone().try_acquire_owned() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "request capacity reached",
        );
    };
    // Health remains available under owner traffic. Other requests have a shared bounded rate.
    if !matches!(request.uri().path(), "/healthz" | "/readyz") {
        let allowed = if let Ok(mut rate) = api.rate.lock() {
            if rate.0.elapsed() >= Duration::from_secs(1) {
                *rate = (Instant::now(), 0);
            }
            if rate.1 >= 64 {
                false
            } else {
                rate.1 += 1;
                true
            }
        } else {
            false
        };
        if !allowed {
            return error(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit",
                "request rate reached",
            );
        }
    }
    // Slow request bodies cannot retain an application permit indefinitely.
    let mut response = match tokio::time::timeout(Duration::from_secs(8), next.run(request)).await {
        Ok(response) => response,
        Err(_) => error(
            StatusCode::REQUEST_TIMEOUT,
            "timeout",
            "request deadline exceeded; reconcile command id if submitted",
        ),
    };
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().expect("static header"));
    response.headers_mut().insert(
        "x-content-type-options",
        "nosniff".parse().expect("static header"),
    );
    response
}
async fn health(State(api): State<Api>) -> Json<Value> {
    Json(
        json!({"service":"mm2-engine","alive":true,"shutting_down":api.handle.state.borrow()["shutting_down"]}),
    )
}
async fn ready(State(api): State<Api>) -> Response {
    let snapshot = api.handle.state.borrow().clone();
    (if snapshot["api_ready"] == true { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE },
        Json(json!({"api_ready":snapshot["api_ready"],"feed_ready":snapshot["feed_ready"],"shadow_ready":snapshot["shadow_ready"],"live_eligible":false}))).into_response()
}
fn authorized(api: &Api, headers: &HeaderMap) -> Option<Response> {
    if api.auth.authorized(headers) {
        None
    } else {
        Some(error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "owner authentication required",
        ))
    }
}
async fn state(State(api): State<Api>, headers: HeaderMap) -> Response {
    if let Some(response) = authorized(&api, &headers) {
        return response;
    }
    Json(api.handle.state.borrow().as_ref().clone()).into_response()
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlRequest {
    id: String,
    expected_version: u64,
    action: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsRequest {
    id: String,
    expected_version: u64,
    limits: Limits,
}
async fn control(
    State(api): State<Api>,
    headers: HeaderMap,
    body: Result<Json<ControlRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Some(response) = authorized(&api, &headers) {
        return response;
    }
    let body = match body {
        Ok(Json(body)) => body,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid",
                "invalid control JSON schema",
            );
        }
    };
    command_result(
        api.handle
            .command(Command::Control {
                id: body.id,
                expected_version: body.expected_version,
                action: body.action,
            })
            .await,
    )
}
async fn limits(
    State(api): State<Api>,
    headers: HeaderMap,
    body: Result<Json<LimitsRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Some(response) = authorized(&api, &headers) {
        return response;
    }
    let body = match body {
        Ok(Json(body)) => body,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid",
                "invalid limits JSON schema",
            );
        }
    };
    command_result(
        api.handle
            .command(Command::Limits {
                id: body.id,
                expected_version: body.expected_version,
                limits: body.limits,
            })
            .await,
    )
}
fn command_result(result: Result<Value, Failure>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(failure) => error(
            match failure.kind {
                "invalid" => StatusCode::BAD_REQUEST,
                "conflict" => StatusCode::CONFLICT,
                _ => StatusCode::SERVICE_UNAVAILABLE,
            },
            failure.kind,
            &failure.message,
        ),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TelegramRequest {
    init_data: String,
}
async fn telegram(
    State(api): State<Api>,
    body: Result<Json<TelegramRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let body = match body {
        Ok(Json(body)) => body,
        Err(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "invalid",
                "invalid authentication JSON schema",
            );
        }
    };
    match api.auth.telegram_session(&body.init_data, wall_ms() / 1000) {
        Ok(token) => {
            Json(json!({"token":token,"token_type":"Bearer","expires_in":300})).into_response()
        }
        Err(_) => error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "Telegram owner authentication failed",
        ),
    }
}
async fn events(State(api): State<Api>, headers: HeaderMap) -> Response {
    if let Some(response) = authorized(&api, &headers) {
        return response;
    }
    let Ok(permit) = api.sse.clone().try_acquire_owned() else {
        return error(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "event stream capacity reached",
        );
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    let events = stream::unfold(
        (api.handle.state.clone(), true, permit, deadline),
        |(mut state, first, permit, deadline)| async move {
            if !first {
                tokio::select! { biased; _ = tokio::time::sleep_until(deadline) => return None,
                changed = state.changed() => { if changed.is_err() { return None; } } }
            }
            let snapshot = state.borrow_and_update().clone();
            let id = format!(
                "{}:{}",
                snapshot["boot_id"].as_str().unwrap_or("unknown"),
                snapshot["sequence"]
            );
            // Always a complete snapshot, including after a reconnect or a coalesced slow reader.
            let event = Event::default()
                .event("snapshot")
                .id(id)
                .data(snapshot.to_string());
            Some((Ok::<_, Infallible>(event), (state, false, permit, deadline)))
        },
    );
    Sse::new(events)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}
fn error(status: StatusCode, kind: &str, message: &str) -> Response {
    (status, Json(json!({"error":kind,"message":message}))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
    #[tokio::test]
    async fn unauthorized_mutations_and_extra_fields_have_no_effect() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::config::Config {
            mode: "shadow".into(),
            feed_source: crate::config::FeedSource::Demo,
            listen: "127.0.0.1:8088".parse().unwrap(),
            allow_remote_http: false,
            state_dir: dir.path().into(),
            journal_max_bytes: 1_048_576,
            command_capacity: 8,
            feed_capacity: 16,
            limits: Limits {
                initial_collateral_usd: "1000".into(),
                max_imbalance_shares: "25".into(),
                lot_shares: "5".into(),
                half_width: "0.045".into(),
                max_feed_age_ms: 1000,
            },
        };
        let (_feed, rx) = tokio::sync::mpsc::channel(16);
        let (_fault, fault) = tokio::sync::watch::channel(None);
        let (stop, shutdown) = tokio::sync::watch::channel(false);
        let (handle, actor) =
            crate::runtime::prepare(&config, Instant::now(), rx, fault, shutdown).unwrap();
        let task = tokio::spawn(actor.run());
        let router = Api::new(handle.clone(), Auth::new("a".repeat(64), None).unwrap()).router();
        let body = r#"{"id":"one","expected_version":0,"action":"resume"}"#;
        let request = |auth: bool, text: &str| {
            let mut builder = Request::builder()
                .uri("/v1/control")
                .method("POST")
                .header("content-type", "application/json");
            if auth {
                builder = builder.header("authorization", format!("Bearer {}", "a".repeat(64)));
            }
            builder.body(Body::from(text.to_string())).unwrap()
        };
        assert_eq!(
            router
                .clone()
                .oneshot(request(false, body))
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            router
                .clone()
                .oneshot(request(
                    true,
                    r#"{"id":"one","expected_version":0,"action":"resume","wallet":"hidden"}"#
                ))
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(handle.state.borrow()["command_version"], 0);
        let response = router.oneshot(request(true, body)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let value: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(value["receipt"]["applied_version"], 1);
        assert_eq!(value["state"]["live_eligible"], false);
        stop.send(true).unwrap();
        task.await.unwrap();
    }
}
