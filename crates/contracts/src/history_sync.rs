use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A per-table high-water mark for the collector's append-only history stream.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct HistorySyncStreamCursor {
    pub stream: String,
    pub row_id: i64,
}

/// A history row as recorded by a collector. `event` uses the source table's
/// column names and intentionally excludes the local SQLite row id.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct HistorySyncRecord {
    pub stream: String,
    pub source_row_id: i64,
    pub event: Value,
}

#[derive(Clone, Debug, Deserialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct HistorySyncExportInput {
    pub user_id: String,
    pub source_id: String,
    #[serde(default)]
    pub cursors: Vec<HistorySyncStreamCursor>,
    #[serde(default = "default_history_sync_limit")]
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct HistorySyncExportOutput {
    pub source_id: String,
    pub records: Vec<HistorySyncRecord>,
    /// High-water marks advanced through the rows included in this page.
    pub cursors: Vec<HistorySyncStreamCursor>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Deserialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct HistorySyncImportInput {
    pub source_id: String,
    pub records: Vec<HistorySyncRecord>,
    /// The page's acknowledged high-water marks. They are committed with its rows.
    #[serde(default)]
    pub cursors: Vec<HistorySyncStreamCursor>,
}

#[derive(Clone, Debug, Serialize, specta::Type)]
#[serde(rename_all = "camelCase")]
pub struct HistorySyncImportOutput {
    pub inserted: u32,
    pub skipped_duplicates: u32,
    pub cursors: Vec<HistorySyncStreamCursor>,
}

pub const HISTORY_SYNC_STREAMS: [&str; 6] = [
    "feed_gps",
    "feed_status",
    "feed_bio",
    "feed_avatar",
    "feed_online_offline",
    "friend_log_history",
];

fn default_history_sync_limit() -> u32 {
    500
}
