#![allow(non_snake_case)]

use serde::Serialize;
use tauri::State;
use vrcx_0_contracts::history_sync::{
    HistorySyncExportOutput, HistorySyncImportInput, HistorySyncStreamCursor, HISTORY_SYNC_STREAMS,
};

use crate::commands::blocking::run_blocking;
use crate::error::AppError;
use crate::state::AppState;

const PAGE_LIMIT: u32 = 500;
const MAX_PAGES_PER_SYNC: usize = 100_000;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct HistorySyncStatus {
    pub imported: u64,
    pub skipped_duplicates: u64,
    pub pages: u32,
    pub source_id: String,
}

#[tauri::command]
#[specta::specta]
pub async fn app__history_sync_now(
    state: State<'_, AppState>,
    server_url: String,
    token: String,
    user_id: String,
) -> Result<HistorySyncStatus, AppError> {
    ensure_current_account(&state, &user_id)?;
    let server_url = normalize_server_url(&server_url)?;
    let token = token.trim().to_owned();
    let user_id = user_id.trim().to_owned();
    if token.is_empty() {
        return Err(AppError::Custom("Collector token is required.".into()));
    }
    if user_id.is_empty() {
        return Err(AppError::Custom(
            "Sign in before syncing collector history.".into(),
        ));
    }

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(|_| AppError::Custom("Could not create the collector sync client.".into()))?;
    let local_data = state.runtime_host().local_data().clone();

    ensure_current_account(&state, &user_id)?;
    let first_page = fetch_page(&client, &server_url, &token, &user_id, &[]).await?;
    let source_id = first_page.source_id.clone();
    if source_id.trim().is_empty() {
        return Err(AppError::Custom(
            "Collector returned an invalid history source id.".into(),
        ));
    }

    // Importing an empty page asks persistence for the durable local checkpoints
    // belonging to this collector. It makes each run incremental without keeping
    // a separate cursor file beside the database.
    let checkpoints = run_blocking("history sync checkpoint lookup", {
        let local_data = local_data.clone();
        let user_id = user_id.clone();
        let source_id = source_id.clone();
        move || {
            local_data.history_sync_import(
                &user_id,
                &HistorySyncImportInput {
                    source_id,
                    records: Vec::new(),
                    cursors: Vec::new(),
                },
            )
        }
    })
    .await?
    .cursors;

    let mut current_cursors = if checkpoints.iter().any(|cursor| cursor.row_id > 0) {
        checkpoints
    } else {
        Vec::new()
    };
    let mut page = if current_cursors.is_empty() {
        first_page
    } else {
        fetch_page(&client, &server_url, &token, &user_id, &current_cursors).await?
    };
    let mut imported = 0_u64;
    let mut skipped_duplicates = 0_u64;
    let mut pages = 0_u32;

    loop {
        ensure_current_account(&state, &user_id)?;
        if page.source_id != source_id {
            return Err(AppError::Custom(
                "Collector history source changed during sync.".into(),
            ));
        }
        pages = pages.saturating_add(1);
        let had_more = page.has_more;
        let page_was_empty = page.records.is_empty();
        let next_cursors = page.cursors.clone();
        let result = run_blocking("history sync page import", {
            let local_data = local_data.clone();
            let user_id = user_id.clone();
            let source_id = source_id.clone();
            let records = page.records;
            move || {
                local_data.history_sync_import(
                    &user_id,
                    &HistorySyncImportInput {
                        source_id,
                        records,
                        cursors: next_cursors,
                    },
                )
            }
        })
        .await?;
        imported = imported.saturating_add(u64::from(result.inserted));
        skipped_duplicates =
            skipped_duplicates.saturating_add(u64::from(result.skipped_duplicates));

        if !had_more {
            break;
        }
        if page_was_empty {
            return Err(AppError::Custom(
                "Collector requested another page without advancing history.".into(),
            ));
        }
        if pages as usize >= MAX_PAGES_PER_SYNC {
            return Err(AppError::Custom(
                "Collector history sync exceeded the per-run page limit.".into(),
            ));
        }
        ensure_cursor_progress(&current_cursors, &result.cursors)?;
        current_cursors = result.cursors.clone();
        ensure_current_account(&state, &user_id)?;
        page = fetch_page(&client, &server_url, &token, &user_id, &current_cursors).await?;
    }

    Ok(HistorySyncStatus {
        imported,
        skipped_duplicates,
        pages,
        source_id,
    })
}

async fn fetch_page(
    client: &reqwest::Client,
    server_url: &str,
    token: &str,
    user_id: &str,
    cursors: &[HistorySyncStreamCursor],
) -> Result<HistorySyncExportOutput, AppError> {
    let url = format!("{server_url}/api/history-sync");
    let cursors_json = serde_json::to_string(cursors)
        .map_err(|_| AppError::Custom("Could not prepare collector history cursors.".into()))?;
    let mut limit = PAGE_LIMIT;
    let response = loop {
        let response = client
            .get(&url)
            .bearer_auth(token)
            .query(&[
                ("user_id", user_id),
                ("limit", &limit.to_string()),
                ("cursors", &cursors_json),
            ])
            .send()
            .await
            .map_err(|_| AppError::Custom("Could not reach the history collector.".into()))?;
        if response.status() == reqwest::StatusCode::PAYLOAD_TOO_LARGE && limit > 1 {
            limit = (limit / 2).max(1);
            continue;
        }
        break response;
    };
    if !response.status().is_success() {
        let message = match response.status() {
            reqwest::StatusCode::UNAUTHORIZED => "Collector rejected the bearer token.",
            reqwest::StatusCode::FORBIDDEN => {
                "Collector account does not match the signed-in account."
            }
            reqwest::StatusCode::NOT_FOUND => "Collector does not provide the history sync API.",
            reqwest::StatusCode::PAYLOAD_TOO_LARGE => {
                "Collector history page is too large to sync."
            }
            _ => "Collector could not export history.",
        };
        return Err(AppError::Custom(message.into()));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(AppError::Custom(
            "Collector returned a history page that is too large.".into(),
        ));
    }
    let mut response = response;
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| AppError::Custom("Could not read the collector history page.".into()))?
    {
        if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(AppError::Custom(
                "Collector returned a history page that is too large.".into(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    let page = serde_json::from_slice::<HistorySyncExportOutput>(&body)
        .map_err(|_| AppError::Custom("Collector returned an invalid history page.".into()))?;
    validate_page(&page)?;
    Ok(page)
}

fn ensure_current_account(state: &AppState, requested_user_id: &str) -> Result<(), AppError> {
    let current_user_id = state
        .runtime_host()
        .require_active_scope("History sync")?
        .current_user_id;
    if current_user_id != requested_user_id {
        return Err(AppError::Custom(
            "Signed-in account changed during collector history sync.".into(),
        ));
    }
    Ok(())
}

fn ensure_cursor_progress(
    previous: &[HistorySyncStreamCursor],
    next: &[HistorySyncStreamCursor],
) -> Result<(), AppError> {
    let previous_rows = previous
        .iter()
        .map(|cursor| (cursor.stream.as_str(), cursor.row_id))
        .collect::<std::collections::HashMap<_, _>>();
    if !next.iter().any(|cursor| {
        cursor.row_id
            > previous_rows
                .get(cursor.stream.as_str())
                .copied()
                .unwrap_or(0)
    }) {
        return Err(AppError::Custom(
            "Collector history page did not advance its cursors.".into(),
        ));
    }
    Ok(())
}

fn normalize_server_url(server_url: &str) -> Result<String, AppError> {
    let server_url = server_url.trim().trim_end_matches('/');
    let parsed = reqwest::Url::parse(server_url)
        .map_err(|_| AppError::Custom("Enter a valid collector server URL.".into()))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(AppError::Custom(
            "Collector URL must be an HTTP or HTTPS origin without credentials or a query.".into(),
        ));
    }
    Ok(server_url.to_owned())
}

fn validate_page(page: &HistorySyncExportOutput) -> Result<(), AppError> {
    if page.source_id.trim().is_empty() {
        return Err(AppError::Custom(
            "Collector returned an invalid history source id.".into(),
        ));
    }
    let mut cursors = std::collections::HashMap::new();
    for cursor in &page.cursors {
        if cursor.row_id < 0
            || !HISTORY_SYNC_STREAMS.contains(&cursor.stream.as_str())
            || cursors
                .insert(cursor.stream.as_str(), cursor.row_id)
                .is_some()
        {
            return Err(AppError::Custom(
                "Collector returned an invalid history cursor.".into(),
            ));
        }
    }
    for record in &page.records {
        if record.source_row_id <= 0
            || !HISTORY_SYNC_STREAMS.contains(&record.stream.as_str())
            || !record.event.is_object()
            || cursors
                .get(record.stream.as_str())
                .is_none_or(|cursor| *cursor < record.source_row_id)
        {
            return Err(AppError::Custom(
                "Collector returned an invalid history record.".into(),
            ));
        }
    }
    Ok(())
}
