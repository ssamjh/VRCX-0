use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{header, header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use tokio::net::TcpListener;
use vrcx_0_contracts::history_sync::{
    HistorySyncExportInput, HistorySyncStreamCursor, HISTORY_SYNC_STREAMS,
};
use vrcx_0_persistence::DatabaseService;

const SOURCE_ID_CONFIG_KEY: &str = "historySyncSourceId";
const DEFAULT_LIMIT: u32 = 500;
const MAX_LIMIT: u32 = 500;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone)]
struct ApiState {
    db: Arc<DatabaseService>,
    bearer_token: Arc<str>,
    source_id: Arc<str>,
    account_user_id: Arc<str>,
}

#[derive(Debug, Deserialize)]
struct PullQuery {
    user_id: String,
    #[serde(default)]
    cursors: String,
    #[serde(default = "default_limit")]
    limit: u32,
}

/// Start the read-only history sync endpoint. The caller must supply the account
/// currently authenticated by the headless runtime, so requests cannot select a
/// different owner table from the same database.
pub async fn serve(
    listener: TcpListener,
    db: Arc<DatabaseService>,
    bearer_token: String,
    source_id: String,
    account_user_id: String,
) -> Result<(), std::io::Error> {
    let state = ApiState {
        db,
        bearer_token: Arc::from(bearer_token),
        source_id: Arc::from(source_id),
        account_user_id: Arc::from(account_user_id),
    };
    axum::serve(listener, router(state)).await
}

fn router(state: ApiState) -> Router {
    Router::new()
        .route("/api/history-sync", get(export_history))
        .with_state(state)
}

async fn export_history(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<PullQuery>,
) -> Response {
    if !authorized(&headers, &state.bearer_token) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if query.user_id != state.account_user_id.as_ref() {
        return StatusCode::FORBIDDEN.into_response();
    }
    let cursors = match if query.cursors.trim().is_empty() {
        Ok(Vec::new())
    } else {
        serde_json::from_str::<Vec<HistorySyncStreamCursor>>(&query.cursors)
    } {
        Ok(cursors) => cursors,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    if cursors.len() > HISTORY_SYNC_STREAMS.len()
        || cursors.iter().any(|cursor| {
            cursor.row_id < 0 || !HISTORY_SYNC_STREAMS.contains(&cursor.stream.as_str())
        })
        || cursors
            .iter()
            .map(|cursor| cursor.stream.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len()
            != cursors.len()
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let input = HistorySyncExportInput {
        user_id: query.user_id,
        source_id: state.source_id.to_string(),
        cursors,
        limit: query.limit.clamp(1, MAX_LIMIT),
    };
    match tokio::task::spawn_blocking(move || {
        vrcx_0_persistence::history_sync::history_sync_export(
            &state.db,
            &input.user_id,
            &input.source_id,
            &input.cursors,
            input.limit,
        )
    })
    .await
    {
        Ok(Ok(output)) => match serde_json::to_vec(&output) {
            Ok(body) if body.len() <= MAX_RESPONSE_BYTES => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response(),
            Ok(_) => StatusCode::PAYLOAD_TOO_LARGE.into_response(),
            Err(error) => {
                tracing::error!(error = %error, "headless history sync serialization failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
        Ok(Err(error)) => {
            tracing::error!(error = %error, "headless history sync export failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Err(error) => {
            tracing::error!(error = %error, "headless history sync task failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn authorized(headers: &HeaderMap, expected_token: &str) -> bool {
    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(token.as_bytes(), expected_token.as_bytes())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let max_len = left.len().max(right.len());
    for index in 0..max_len {
        difference |= usize::from(left.get(index).copied().unwrap_or_default())
            ^ usize::from(right.get(index).copied().unwrap_or_default());
    }
    difference == 0
}

fn default_limit() -> u32 {
    DEFAULT_LIMIT
}

pub fn configured_listener() -> Result<Option<(TcpListener, String)>, String> {
    let token = match std::env::var("VRCX_COLLECTOR_TOKEN") {
        Ok(token) if !token.trim().is_empty() => token.trim().to_owned(),
        Ok(_) => return Err("VRCX_COLLECTOR_TOKEN cannot be empty when configured".into()),
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(error) => return Err(format!("invalid VRCX_COLLECTOR_TOKEN: {error}")),
    };
    if token == "replace-with-a-long-random-token" {
        return Err(
            "Replace the example VRCX_COLLECTOR_TOKEN before starting the collector".into(),
        );
    }
    let bind = std::env::var("VRCX_COLLECTOR_BIND").unwrap_or_else(|_| "127.0.0.1:9001".to_owned());
    let address = bind
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid VRCX_COLLECTOR_BIND: {error}"))?;
    let listener = std::net::TcpListener::bind(address)
        .map_err(|error| format!("failed to bind collector API at {address}: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("failed to configure collector API listener: {error}"))?;
    let listener = TcpListener::from_std(listener)
        .map_err(|error| format!("failed to start collector API listener: {error}"))?;
    Ok(Some((listener, token)))
}

pub fn load_or_create_source_id(db: &DatabaseService, reset: bool) -> Result<String, String> {
    let existing = vrcx_0_persistence::config::get_string(db, SOURCE_ID_CONFIG_KEY, "")
        .map_err(|error| format!("failed to read history sync source id: {error}"))?;
    if !reset && !existing.trim().is_empty() {
        return Ok(existing);
    }
    let source_id = uuid::Uuid::new_v4().to_string();
    vrcx_0_persistence::config::set_string(db, SOURCE_ID_CONFIG_KEY, &source_id)
        .map_err(|error| format!("failed to persist history sync source id: {error}"))?;
    Ok(source_id)
}
