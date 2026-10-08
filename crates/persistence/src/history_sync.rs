use serde_json::Value;
use vrcx_0_contracts::history_sync::{
    HistorySyncExportOutput, HistorySyncImportInput, HistorySyncImportOutput, HistorySyncRecord,
    HistorySyncStreamCursor,
};

mod merge;
pub use merge::history_sync_import_reconciled;
pub(crate) use merge::history_sync_reconcile_pending;

use crate::common::ParamsBuilder;
use crate::database::{DatabaseService, DatabaseWriteTransaction};
use crate::realtime::{ensure_realtime_tables, normalize_user_table_prefix};
use crate::Error;

const HISTORY_SYNC_SCHEMA_KEY: &str = "history-sync-checkpoint-v1";
const HISTORY_SYNC_JOURNAL_SCHEMA_KEY: &str = "history-sync-journal-v1";
const MAX_PAGE_SIZE: usize = 2_000;

#[derive(Clone, Copy)]
struct StreamSpec {
    name: &'static str,
    columns: &'static [&'static str],
}

const STREAMS: [StreamSpec; 6] = [
    StreamSpec {
        name: "feed_gps",
        columns: &[
            "created_at",
            "user_id",
            "display_name",
            "location",
            "world_name",
            "previous_location",
            "time",
            "group_name",
        ],
    },
    StreamSpec {
        name: "feed_status",
        columns: &[
            "created_at",
            "user_id",
            "display_name",
            "status",
            "status_description",
            "previous_status",
            "previous_status_description",
        ],
    },
    StreamSpec {
        name: "feed_bio",
        columns: &[
            "created_at",
            "user_id",
            "display_name",
            "bio",
            "previous_bio",
        ],
    },
    StreamSpec {
        name: "feed_avatar",
        columns: &[
            "created_at",
            "user_id",
            "display_name",
            "owner_id",
            "avatar_name",
            "current_avatar_image_url",
            "current_avatar_thumbnail_image_url",
            "previous_current_avatar_image_url",
            "previous_current_avatar_thumbnail_image_url",
        ],
    },
    StreamSpec {
        name: "feed_online_offline",
        columns: &[
            "created_at",
            "user_id",
            "display_name",
            "type",
            "location",
            "world_name",
            "time",
            "group_name",
        ],
    },
    StreamSpec {
        name: "friend_log_history",
        columns: &[
            "created_at",
            "type",
            "user_id",
            "display_name",
            "previous_display_name",
            "trust_level",
            "previous_trust_level",
            "friend_number",
        ],
    },
];

/// Read an append-only page from the collector's per-table history streams.
pub fn history_sync_export(
    db: &DatabaseService,
    user_id: &str,
    source_id: &str,
    cursors: &[HistorySyncStreamCursor],
    limit: u32,
) -> Result<HistorySyncExportOutput, Error> {
    let prefix = normalize_user_table_prefix(user_id)?;
    history_sync_prepare(db, user_id)?;
    if source_id.trim().is_empty() {
        return Err(Error::Database(
            "History sync source id is required.".into(),
        ));
    }
    let page_size = (limit as usize).clamp(1, MAX_PAGE_SIZE);
    let mut records = Vec::new();
    let mut result_cursors = Vec::with_capacity(STREAMS.len());
    let mut has_more = false;

    for stream in STREAMS {
        let after = cursor_for(cursors, stream.name)?;
        let args = ParamsBuilder::new()
            .set("owner", prefix.clone())
            .set("stream", stream.name)
            .set("after", after)
            .set("limit", (page_size + 1) as i64)
            .build();
        let rows = db.execute(
            "SELECT sequence, event_json FROM history_sync_journal WHERE owner_prefix = @owner AND stream = @stream AND sequence > @after ORDER BY sequence ASC LIMIT @limit",
            &args,
        )?;
        has_more |= rows.len() > page_size;
        for row in rows.into_iter().take(page_size) {
            let source_row_id = row[0]
                .as_i64()
                .ok_or_else(|| Error::Database("Invalid history sync journal sequence.".into()))?;
            let event: Value = row[1]
                .as_str()
                .ok_or_else(|| Error::Database("Invalid history sync journal event.".into()))
                .and_then(|json| {
                    serde_json::from_str(json).map_err(|error| {
                        Error::Database(format!("Invalid history sync journal event: {error}"))
                    })
                })?;
            if !event.is_object() {
                return Err(Error::Database(
                    "Invalid history sync journal event.".into(),
                ));
            }
            records.push(HistorySyncRecord {
                stream: stream.name.to_owned(),
                source_row_id,
                event,
            });
        }
        let advanced = records
            .iter()
            .filter(|record| record.stream == stream.name)
            .map(|record| record.source_row_id)
            .max()
            .unwrap_or(after);
        result_cursors.push(HistorySyncStreamCursor {
            stream: stream.name.to_owned(),
            row_id: advanced,
        });
    }

    // Stable ordering makes pages predictable while each stream retains an independent cursor.
    records.sort_by(|left, right| {
        let left_time = left
            .event
            .get("created_at")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let right_time = right
            .event
            .get("created_at")
            .and_then(Value::as_str)
            .unwrap_or_default();
        left_time
            .cmp(right_time)
            .then_with(|| left.stream.cmp(&right.stream))
            .then_with(|| left.source_row_id.cmp(&right.source_row_id))
    });

    Ok(HistorySyncExportOutput {
        source_id: source_id.to_owned(),
        records,
        cursors: result_cursors,
        has_more,
    })
}

/// Initialize durable source journaling before a headless collector begins recording.
pub fn history_sync_prepare(db: &DatabaseService, user_id: &str) -> Result<(), Error> {
    let prefix = normalize_user_table_prefix(user_id)?;
    ensure_realtime_tables(db, &prefix)?;
    ensure_history_journal(db, &prefix)?;
    Ok(())
}

/// Import one collector page. Event inserts and cursor advancement share one SQLite transaction,
/// so a retry either repeats the same idempotent page or observes its committed checkpoint.
pub fn history_sync_import(
    db: &DatabaseService,
    user_id: &str,
    input: &HistorySyncImportInput,
) -> Result<HistorySyncImportOutput, Error> {
    let prefix = normalize_user_table_prefix(user_id)?;
    ensure_realtime_tables(db, &prefix)?;
    ensure_checkpoint_table(db)?;
    if input.source_id.trim().is_empty() {
        return Err(Error::Database(
            "History sync source id is required.".into(),
        ));
    }
    validate_cursors(&input.cursors)?;
    for record in &input.records {
        stream_spec(&record.stream)?;
        validate_event(record)?;
    }

    db.write_transaction(|tx| {
        let mut inserted = 0_u32;
        let mut skipped_duplicates = 0_u32;
        for record in &input.records {
            let stream = stream_spec(&record.stream)?;
            let table = format!("{prefix}_{}", stream.name);
            let mut where_parts = Vec::with_capacity(stream.columns.len());
            let mut args = ParamsBuilder::new();
            for column in stream.columns {
                let parameter = format!("event_{column}");
                where_parts.push(format!("{column} IS @{parameter}"));
                args = args.set(&parameter, record.event.get(*column).cloned().unwrap_or(Value::Null));
            }
            let existing = tx.execute(
                &format!("SELECT id FROM {table} WHERE {} LIMIT 1", where_parts.join(" AND ")),
                &args.build(),
            )?;
            if !existing.is_empty() {
                skipped_duplicates += 1;
                continue;
            }
            let columns = stream.columns.join(", ");
            let placeholders = stream.columns.iter().map(|column| format!("@event_{column}")).collect::<Vec<_>>().join(", ");
            let mut args = ParamsBuilder::new();
            for column in stream.columns {
                args = args.set(&format!("event_{column}"), record.event.get(*column).cloned().unwrap_or(Value::Null));
            }
            tx.execute_non_query(&format!("INSERT INTO {table} ({columns}) VALUES ({placeholders})"), &args.build())?;
            inserted += 1;
        }

        for cursor in &input.cursors {
            let previous = tx.execute(
                "SELECT row_id FROM history_sync_checkpoint WHERE owner_prefix = @owner AND source_id = @source AND stream = @stream",
                &ParamsBuilder::new().set("owner", prefix.clone()).set("source", input.source_id.clone()).set("stream", cursor.stream.clone()).build(),
            )?;
            let current = previous.first().and_then(|row| row[0].as_i64()).unwrap_or(0);
            let page_max = input
                .records
                .iter()
                .filter(|record| record.stream == cursor.stream)
                .map(|record| record.source_row_id)
                .max()
                .unwrap_or(0);
            if cursor.row_id > current && cursor.row_id > page_max {
                return Err(Error::Database(format!(
                    "History sync cursor for {} advances beyond the imported page.",
                    cursor.stream
                )));
            }
            if cursor.row_id > current {
                tx.execute_non_query(
                    "INSERT INTO history_sync_checkpoint (owner_prefix, source_id, stream, row_id) VALUES (@owner, @source, @stream, @row_id) ON CONFLICT(owner_prefix, source_id, stream) DO UPDATE SET row_id = excluded.row_id",
                    &ParamsBuilder::new().set("owner", prefix.clone()).set("source", input.source_id.clone()).set("stream", cursor.stream.clone()).set("row_id", cursor.row_id).build(),
                )?;
            }
        }
        let checkpoints = load_checkpoints(tx, &prefix, &input.source_id)?;
        Ok(HistorySyncImportOutput { inserted, skipped_duplicates, cursors: checkpoints })
    })
}

fn ensure_checkpoint_table(db: &DatabaseService) -> Result<(), Error> {
    db.ensure_schema_once(HISTORY_SYNC_SCHEMA_KEY, || {
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_checkpoint (owner_prefix TEXT NOT NULL, source_id TEXT NOT NULL, stream TEXT NOT NULL, row_id INTEGER NOT NULL, PRIMARY KEY(owner_prefix, source_id, stream))",
            &Default::default(),
        )?;
        Ok(())
    })
}

fn ensure_history_journal(db: &DatabaseService, prefix: &str) -> Result<(), Error> {
    // This is an archive journal: exported history remains available after source-table cleanup,
    // so a desktop that has been offline can still catch up. We cannot prune by one client's
    // cursor because a collector may serve more than one desktop with independent checkpoints.
    db.ensure_schema_once(HISTORY_SYNC_JOURNAL_SCHEMA_KEY, || {
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_journal (sequence INTEGER PRIMARY KEY AUTOINCREMENT, owner_prefix TEXT NOT NULL, stream TEXT NOT NULL, source_row_id INTEGER NOT NULL, event_json TEXT NOT NULL)",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_journal_init (owner_prefix TEXT NOT NULL, stream TEXT NOT NULL, PRIMARY KEY(owner_prefix, stream))",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE INDEX IF NOT EXISTS history_sync_journal_owner_stream_sequence_idx ON history_sync_journal (owner_prefix, stream, sequence)",
            &Default::default(),
        )?;
        Ok(())
    })?;
    for stream in STREAMS {
        let table = format!("{prefix}_{}", stream.name);
        let trigger = format!("{prefix}_history_sync_{}_insert", stream.name);
        let trigger_values = stream
            .columns
            .iter()
            .map(|column| format!("'{column}', NEW.{column}"))
            .collect::<Vec<_>>()
            .join(", ");
        let seed_values = stream
            .columns
            .iter()
            .map(|column| format!("'{column}', {column}"))
            .collect::<Vec<_>>()
            .join(", ");
        let args = ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("stream", stream.name)
            .set("trigger", trigger.clone())
            .build();
        let prepared = db.execute(
            "SELECT 1 FROM history_sync_journal_init i JOIN sqlite_master m ON m.type = 'trigger' AND m.name = @trigger WHERE i.owner_prefix = @owner AND i.stream = @stream LIMIT 1",
            &args,
        )?;
        if !prepared.is_empty() {
            continue;
        }
        db.write_transaction(|tx| {
            let args = ParamsBuilder::new()
                .set("owner", prefix.to_owned())
                .set("stream", stream.name)
                .build();
            let initialized = tx.execute(
                "SELECT stream FROM history_sync_journal_init WHERE owner_prefix = @owner AND stream = @stream",
                &args,
            )?;
            if initialized.is_empty() {
                tx.execute_non_query(
                    &format!("INSERT INTO history_sync_journal (owner_prefix, stream, source_row_id, event_json) SELECT @owner, @stream, id, json_object({seed_values}) FROM {table} ORDER BY id"),
                    &args,
                )?;
                tx.execute_non_query(
                    "INSERT INTO history_sync_journal_init (owner_prefix, stream) VALUES (@owner, @stream)",
                    &args,
                )?;
            }
            tx.execute_non_query(
                &format!("CREATE TRIGGER IF NOT EXISTS {trigger} AFTER INSERT ON {table} BEGIN INSERT INTO history_sync_journal (owner_prefix, stream, source_row_id, event_json) VALUES ('{prefix}', '{stream_name}', NEW.id, json_object({trigger_values})); END", stream_name = stream.name),
                &Default::default(),
            )?;
            Ok(())
        })?;
    }
    Ok(())
}

fn load_checkpoints(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
) -> Result<Vec<HistorySyncStreamCursor>, Error> {
    let rows = tx.execute(
        "SELECT stream, row_id FROM history_sync_checkpoint WHERE owner_prefix = @owner AND source_id = @source ORDER BY stream",
        &ParamsBuilder::new().set("owner", prefix.to_owned()).set("source", source_id.to_owned()).build(),
    )?;
    rows.into_iter()
        .map(|row| {
            let stream = row[0]
                .as_str()
                .ok_or_else(|| Error::Database("Invalid history sync stream checkpoint.".into()))?
                .to_owned();
            let row_id = row[1]
                .as_i64()
                .ok_or_else(|| Error::Database("Invalid history sync checkpoint.".into()))?;
            Ok(HistorySyncStreamCursor { stream, row_id })
        })
        .collect()
}

fn cursor_for(cursors: &[HistorySyncStreamCursor], stream: &str) -> Result<i64, Error> {
    let mut value = 0;
    for cursor in cursors.iter().filter(|cursor| cursor.stream == stream) {
        if cursor.row_id < 0 {
            return Err(Error::Database(
                "History sync cursor cannot be negative.".into(),
            ));
        }
        value = value.max(cursor.row_id);
    }
    Ok(value)
}

fn validate_cursors(cursors: &[HistorySyncStreamCursor]) -> Result<(), Error> {
    for cursor in cursors {
        stream_spec(&cursor.stream)?;
        if cursor.row_id < 0 {
            return Err(Error::Database(
                "History sync cursor cannot be negative.".into(),
            ));
        }
    }
    Ok(())
}

fn stream_spec(stream: &str) -> Result<&'static StreamSpec, Error> {
    STREAMS
        .iter()
        .find(|spec| spec.name == stream)
        .ok_or_else(|| Error::Database(format!("Unsupported history sync stream: {stream}")))
}

fn validate_event(record: &HistorySyncRecord) -> Result<(), Error> {
    let stream = stream_spec(&record.stream)?;
    let Some(object) = record.event.as_object() else {
        return Err(Error::Database(
            "History sync event must be an object.".into(),
        ));
    };
    if record.source_row_id < 0
        || stream
            .columns
            .iter()
            .any(|column| !object.contains_key(*column))
    {
        return Err(Error::Database(
            "History sync event is incomplete or has an invalid source row id.".into(),
        ));
    }
    Ok(())
}
