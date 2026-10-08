use std::collections::BTreeMap;

use serde_json::{json, Value};
use vrcx_0_contracts::history_sync::{
    HistorySyncImportInput, HistorySyncImportOutput, HistorySyncRecord, HistorySyncStreamCursor,
};

use crate::common::ParamsBuilder;
use crate::database::{DatabaseService, DatabaseWriteTransaction};
use crate::realtime::{ensure_realtime_tables, normalize_user_table_prefix};
use crate::Error;

use super::{stream_spec, validate_cursors, validate_event};

const RECONCILED_SCHEMA_KEY: &str = "history-sync-reconciled-v1";
const MAX_LOCAL_CANDIDATES: i64 = 2_000;

#[derive(Clone)]
struct LocalCandidate {
    sequence: i64,
    row_id: i64,
    signature: String,
}

#[derive(Clone)]
struct RemoteEvent {
    source_sequence: i64,
    record: HistorySyncRecord,
    user_id: String,
    signature: String,
}

/// Import a collector page while reconciling ordered semantic transitions that
/// were independently recorded by this desktop. Remote row identity, rather
/// than payload contents, makes retries safe if timestamps or metadata change.
///
/// The upstream API provides no shared event IDs or epoch, so a repeated
/// identical transition cannot always be distinguished from a second copy of
/// the same observation. Reconciliation is therefore a bounded, ordered
/// semantic match; ambiguous recurrences cannot be distinguished without
/// stable upstream event IDs. Exact payload matches are checked independently
/// of the bounded sequence window before inserting a new physical row.
pub fn history_sync_import_reconciled(
    db: &DatabaseService,
    user_id: &str,
    input: &HistorySyncImportInput,
) -> Result<HistorySyncImportOutput, Error> {
    let prefix = normalize_user_table_prefix(user_id)?;
    ensure_realtime_tables(db, &prefix)?;
    // The desktop journal supplies durable local sequence numbers and also
    // records imported rows. The latter are tagged in the mapping table below.
    super::history_sync_prepare(db, user_id)?;
    ensure_reconciled_schema(db)?;
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
        let initialized = reconciled_source_initialized(tx, &prefix, &input.source_id)?;
        // Existing installations may have old exact-import cursors. Returning
        // no cursors once makes the caller replay the collector journal without
        // resetting or regressing those legacy checkpoints.
        if !initialized && input.records.is_empty() && input.cursors.is_empty() {
            return Ok(HistorySyncImportOutput {
                inserted: 0,
                skipped_duplicates: 0,
                cursors: Vec::new(),
            });
        }
        mark_reconciled_source_initialized(tx, &prefix, &input.source_id)?;

        let mut inserted = 0_u32;
        let mut skipped_duplicates = 0_u32;
        let mut groups = BTreeMap::<(String, String), Vec<RemoteEvent>>::new();
        let mut identities = BTreeMap::<(String, i64), &HistorySyncRecord>::new();
        for record in &input.records {
            match identities.get(&(record.stream.clone(), record.source_row_id)) {
                Some(previous) if previous.event != record.event => {
                    return Err(Error::Database(
                        "History sync page contains conflicting duplicate source records.".into(),
                    ));
                }
                Some(_) => continue,
                None => {
                    identities.insert((record.stream.clone(), record.source_row_id), record);
                }
            }
            if remote_mapping_exists(
                tx,
                &prefix,
                &input.source_id,
                &record.stream,
                record.source_row_id,
            )? {
                skipped_duplicates = skipped_duplicates.saturating_add(1);
                continue;
            }
            let user_id = record
                .event
                .get("user_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned();
            let signature = semantic_signature(record)?;
            groups
                .entry((record.stream.clone(), user_id.clone()))
                .or_default()
                .push(RemoteEvent {
                    source_sequence: record.source_row_id,
                    record: record.clone(),
                    user_id,
                    signature,
                });
        }

        for ((stream, event_user_id), mut remote) in groups {
            remote.sort_by_key(|record| record.source_sequence);
            let _baseline = ensure_local_baseline(tx, &prefix, &input.source_id, &stream)?;
            let anchor = local_anchor(
                tx,
                &prefix,
                &input.source_id,
                &stream,
                &event_user_id,
            )?;
            let candidates = load_local_candidates(
                tx,
                &prefix,
                &input.source_id,
                &stream,
                &event_user_id,
                anchor,
            )?;
            let remote_signatures = remote
                .iter()
                .map(|record| record.signature.as_str())
                .collect::<Vec<_>>();
            let local_signatures = candidates
                .iter()
                .map(|candidate| candidate.signature.as_str())
                .collect::<Vec<_>>();
            let matches = ordered_alignment(&remote_signatures, &local_signatures);
            let mut matched_local = vec![None; remote.len()];
            for (remote_index, local_index) in matches {
                matched_local[remote_index] = Some(local_index);
            }

            let mut latest_anchor = anchor;
            for (index, event) in remote.iter().enumerate() {
                if let Some(local_index) = matched_local[index] {
                    let local = &candidates[local_index];
                    insert_remote_mapping(
                        tx,
                        &prefix,
                        &input.source_id,
                        event,
                        local.sequence,
                        local.row_id,
                        false,
                    )?;
                    latest_anchor = latest_anchor.max(local.sequence);
                    skipped_duplicates = skipped_duplicates.saturating_add(1);
                } else {
                    if let Some(row_id) = find_exact_row_id(tx, &prefix, &stream, &event.record)? {
                        let local_sequence = local_journal_sequence(tx, &prefix, &stream, row_id)?;
                        tx.execute_non_query(
                            "INSERT OR IGNORE INTO history_sync_imported_journal (owner_prefix, source_id, stream, sequence) VALUES (@owner, @source, @stream, @sequence)",
                            &ParamsBuilder::new()
                                .set("owner", prefix.clone())
                                .set("source", input.source_id.clone())
                                .set("stream", stream.clone())
                                .set("sequence", local_sequence)
                                .build(),
                        )?;
                        insert_remote_mapping(
                            tx,
                            &prefix,
                            &input.source_id,
                            event,
                            local_sequence,
                            row_id,
                            false,
                        )?;
                        latest_anchor = latest_anchor.max(local_sequence);
                        skipped_duplicates = skipped_duplicates.saturating_add(1);
                        continue;
                    }
                    let row_id = insert_history_record(tx, &prefix, &event.record)?;
                    let local_sequence = local_journal_sequence(tx, &prefix, &stream, row_id)?;
                    tx.execute_non_query(
                        "INSERT OR IGNORE INTO history_sync_imported_journal (owner_prefix, source_id, stream, sequence) VALUES (@owner, @source, @stream, @sequence)",
                        &ParamsBuilder::new()
                            .set("owner", prefix.clone())
                            .set("source", input.source_id.clone())
                            .set("stream", stream.clone())
                            .set("sequence", local_sequence)
                            .build(),
                    )?;
                    insert_remote_mapping(
                        tx,
                        &prefix,
                        &input.source_id,
                        event,
                        local_sequence,
                        row_id,
                        true,
                    )?;
                    inserted = inserted.saturating_add(1);
                }
            }
            if latest_anchor > anchor {
                save_local_anchor(
                    tx,
                    &prefix,
                    &input.source_id,
                    &stream,
                    &event_user_id,
                    latest_anchor,
                )?;
            }
        }

        validate_page_cursors(tx, &prefix, &input.source_id, &input.records, &input.cursors)?;
        save_page_cursors(tx, &prefix, &input.source_id, &input.cursors)?;
        skipped_duplicates = skipped_duplicates.saturating_add(
            history_sync_reconcile_pending(tx, &prefix)?.min(u64::from(u32::MAX)) as u32,
        );
        let cursors = load_page_cursors(tx, &prefix, &input.source_id)?;
        Ok(HistorySyncImportOutput {
            inserted,
            skipped_duplicates,
            cursors,
        })
    })
}

/// Reconcile late local observations against imported collector observations.
/// Call this after the writer has inserted local history rows in the same tx.
/// Imported rows are removed only after a one-to-one semantic sequence match;
/// their original payload remains archived in the append-only journal.
pub(crate) fn history_sync_reconcile_pending(
    tx: &mut DatabaseWriteTransaction<'_>,
    prefix: &str,
) -> Result<u64, Error> {
    if !table_exists(tx, "history_sync_remote_map")? {
        return Ok(0);
    }
    let pending = tx.execute(
        "SELECT DISTINCT source_id, stream, user_id FROM history_sync_remote_map WHERE owner_prefix = @owner AND pending_local = 1 ORDER BY source_id, stream, user_id",
        &ParamsBuilder::new().set("owner", prefix.to_owned()).build(),
    )?;
    let mut reconciled = 0_u64;
    for row in pending {
        let source_id = row[0].as_str().unwrap_or_default().to_owned();
        let stream = row[1].as_str().unwrap_or_default().to_owned();
        let user_id = row[2].as_str().unwrap_or_default().to_owned();
        let baseline = local_baseline(tx, prefix, &source_id, &stream)?;
        let anchor = local_anchor(tx, prefix, &source_id, &stream, &user_id)?.max(baseline);
        let candidates = load_local_candidates(tx, prefix, &source_id, &stream, &user_id, anchor)?;
        if candidates.is_empty() {
            continue;
        }
        let pending_rows = tx.execute(
            "SELECT source_sequence, local_sequence, local_row_id, event_json FROM history_sync_remote_map WHERE owner_prefix = @owner AND source_id = @source AND stream = @stream AND user_id = @user AND pending_local = 1 ORDER BY source_sequence DESC LIMIT @limit",
            &ParamsBuilder::new()
                .set("owner", prefix.to_owned())
                .set("source", source_id.clone())
                .set("stream", stream.clone())
                .set("user", user_id.clone())
                .set("limit", MAX_LOCAL_CANDIDATES)
                .build(),
        )?;
        let mut pending_rows = pending_rows;
        pending_rows.reverse();
        let mut remote = Vec::with_capacity(pending_rows.len());
        for pending_row in pending_rows {
            let source_sequence = pending_row[0].as_i64().unwrap_or_default();
            let imported_local_sequence = pending_row[1].as_i64().unwrap_or_default();
            let imported_row_id = pending_row[2].as_i64().unwrap_or_default();
            let event_json = pending_row[3].as_str().unwrap_or_default();
            let event: Value = serde_json::from_str(event_json).map_err(|error| {
                Error::Database(format!("Invalid mapped history event: {error}"))
            })?;
            let record = HistorySyncRecord {
                stream: stream.clone(),
                source_row_id: source_sequence,
                event,
            };
            remote.push((
                RemoteEvent {
                    source_sequence,
                    user_id: user_id.clone(),
                    signature: semantic_signature(&record)?,
                    record,
                },
                imported_local_sequence,
                imported_row_id,
            ));
        }
        let remote_signatures = remote
            .iter()
            .map(|(record, _, _)| record.signature.as_str())
            .collect::<Vec<_>>();
        let local_signatures = candidates
            .iter()
            .map(|candidate| candidate.signature.as_str())
            .collect::<Vec<_>>();
        if !remote_signatures
            .iter()
            .any(|signature| local_signatures.contains(signature))
        {
            continue;
        }
        let matches = ordered_alignment(&remote_signatures, &local_signatures);
        let mut latest_anchor = anchor;
        for (remote_index, local_index) in matches {
            let (record, imported_local_sequence, imported_row_id) = &remote[remote_index];
            let local = &candidates[local_index];
            if *imported_row_id != local.row_id {
                let table = format!("{prefix}_{}", record.record.stream);
                let spec = stream_spec(&record.record.stream)?;
                let latest_journal = tx.execute(
                    "SELECT sequence FROM history_sync_journal WHERE owner_prefix = @owner AND stream = @stream AND source_row_id = @row_id ORDER BY sequence DESC LIMIT 1",
                    &ParamsBuilder::new()
                        .set("owner", prefix.to_owned())
                        .set("stream", stream.clone())
                        .set("row_id", *imported_row_id)
                        .build(),
                )?;
                let latest_sequence = latest_journal
                    .first()
                    .and_then(|row| row[0].as_i64())
                    .unwrap_or_default();
                let conditions = spec
                    .columns
                    .iter()
                    .map(|column| format!("{column} IS @event_{column}"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let mut args = ParamsBuilder::new().set("id", *imported_row_id);
                for column in spec.columns {
                    args = args.set(
                        &format!("event_{column}"),
                        record
                            .record
                            .event
                            .get(*column)
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                }
                if latest_sequence == *imported_local_sequence {
                    tx.execute_non_query(
                        &format!("DELETE FROM {table} WHERE id = @id AND {conditions}"),
                        &args.build(),
                    )?;
                }
            }
            // Keep every source identity attached to the chosen canonical local
            // row before the imported physical copy is removed.
            tx.execute_non_query(
                "UPDATE history_sync_remote_map SET local_sequence = @local_sequence, local_row_id = @local_row_id, pending_local = 0 WHERE owner_prefix = @owner AND stream = @stream AND local_sequence = @old_local_sequence",
                &ParamsBuilder::new()
                    .set("local_sequence", local.sequence)
                    .set("local_row_id", local.row_id)
                    .set("owner", prefix.to_owned())
                    .set("stream", stream.clone())
                    .set("old_local_sequence", *imported_local_sequence)
                    .build(),
            )?;
            // Also ensure this source identity is linked if another source map
            // had already shared the imported observation.
            tx.execute_non_query(
                "UPDATE history_sync_remote_map SET local_sequence = @local_sequence, local_row_id = @local_row_id, pending_local = 0 WHERE owner_prefix = @owner AND source_id = @source AND stream = @stream AND source_sequence = @source_sequence",
                &ParamsBuilder::new()
                    .set("local_sequence", local.sequence)
                    .set("local_row_id", local.row_id)
                    .set("owner", prefix.to_owned())
                    .set("source", source_id.clone())
                    .set("stream", stream.clone())
                    .set("source_sequence", record.source_sequence)
                    .build(),
            )?;
            latest_anchor = latest_anchor.max(local.sequence);
            reconciled = reconciled.saturating_add(1);
        }
        if latest_anchor > anchor {
            save_local_anchor(tx, prefix, &source_id, &stream, &user_id, latest_anchor)?;
        }
    }
    Ok(reconciled)
}

fn ensure_reconciled_schema(db: &DatabaseService) -> Result<(), Error> {
    db.ensure_schema_once(RECONCILED_SCHEMA_KEY, || {
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_remote_map (owner_prefix TEXT NOT NULL, source_id TEXT NOT NULL, stream TEXT NOT NULL, source_sequence INTEGER NOT NULL, user_id TEXT NOT NULL, local_sequence INTEGER NOT NULL, local_row_id INTEGER NOT NULL, semantic_key TEXT NOT NULL, event_json TEXT NOT NULL, pending_local INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(owner_prefix, source_id, stream, source_sequence))",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE INDEX IF NOT EXISTS history_sync_remote_map_local_idx ON history_sync_remote_map (owner_prefix, source_id, stream, user_id, local_sequence)",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE INDEX IF NOT EXISTS history_sync_remote_map_pending_idx ON history_sync_remote_map (owner_prefix, pending_local, source_id, stream, user_id, source_sequence)",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_imported_journal (owner_prefix TEXT NOT NULL, source_id TEXT NOT NULL, stream TEXT NOT NULL, sequence INTEGER NOT NULL, PRIMARY KEY(owner_prefix, source_id, stream, sequence))",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_reconciled_checkpoint (owner_prefix TEXT NOT NULL, source_id TEXT NOT NULL, stream TEXT NOT NULL, row_id INTEGER NOT NULL, PRIMARY KEY(owner_prefix, source_id, stream))",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_local_anchor (owner_prefix TEXT NOT NULL, source_id TEXT NOT NULL, stream TEXT NOT NULL, user_id TEXT NOT NULL, local_sequence INTEGER NOT NULL, PRIMARY KEY(owner_prefix, source_id, stream, user_id))",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_local_baseline (owner_prefix TEXT NOT NULL, source_id TEXT NOT NULL, stream TEXT NOT NULL, local_sequence INTEGER NOT NULL, PRIMARY KEY(owner_prefix, source_id, stream))",
            &Default::default(),
        )?;
        db.execute_non_query(
            "CREATE TABLE IF NOT EXISTS history_sync_reconciled_source (owner_prefix TEXT NOT NULL, source_id TEXT NOT NULL, initialized INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(owner_prefix, source_id))",
            &Default::default(),
        )?;
        Ok(())
    })
}

fn table_exists(tx: &DatabaseWriteTransaction<'_>, table: &str) -> Result<bool, Error> {
    let rows = tx.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = @name LIMIT 1",
        &ParamsBuilder::new().set("name", table.to_owned()).build(),
    )?;
    Ok(!rows.is_empty())
}

fn reconciled_source_initialized(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
) -> Result<bool, Error> {
    let rows = tx.execute(
        "SELECT 1 FROM history_sync_reconciled_source WHERE owner_prefix = @owner AND source_id = @source LIMIT 1",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .build(),
    )?;
    Ok(!rows.is_empty())
}

fn mark_reconciled_source_initialized(
    tx: &mut DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
) -> Result<(), Error> {
    tx.execute_non_query(
        "INSERT OR IGNORE INTO history_sync_reconciled_source (owner_prefix, source_id) VALUES (@owner, @source)",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .build(),
    )?;
    Ok(())
}

fn remote_mapping_exists(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    stream: &str,
    source_sequence: i64,
) -> Result<bool, Error> {
    let rows = tx.execute(
        "SELECT 1 FROM history_sync_remote_map WHERE owner_prefix = @owner AND source_id = @source AND stream = @stream AND source_sequence = @sequence LIMIT 1",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .set("stream", stream.to_owned())
            .set("sequence", source_sequence)
            .build(),
    )?;
    Ok(!rows.is_empty())
}

fn local_anchor(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    stream: &str,
    user_id: &str,
) -> Result<i64, Error> {
    let rows = tx.execute(
        "SELECT local_sequence FROM history_sync_local_anchor WHERE owner_prefix = @owner AND source_id = @source AND stream = @stream AND user_id = @user LIMIT 1",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .set("stream", stream.to_owned())
            .set("user", user_id.to_owned())
            .build(),
    )?;
    Ok(rows.first().and_then(|row| row[0].as_i64()).unwrap_or(0))
}

fn ensure_local_baseline(
    tx: &mut DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    stream: &str,
) -> Result<i64, Error> {
    let rows = tx.execute(
        "SELECT local_sequence FROM history_sync_local_baseline WHERE owner_prefix = @owner AND source_id = @source AND stream = @stream LIMIT 1",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .set("stream", stream.to_owned())
            .build(),
    )?;
    if let Some(sequence) = rows.first().and_then(|row| row[0].as_i64()) {
        return Ok(sequence);
    }
    let rows = tx.execute(
        "SELECT COALESCE(MAX(sequence), 0) FROM history_sync_journal WHERE owner_prefix = @owner AND stream = @stream",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("stream", stream.to_owned())
            .build(),
    )?;
    let sequence = rows.first().and_then(|row| row[0].as_i64()).unwrap_or(0);
    tx.execute_non_query(
        "INSERT INTO history_sync_local_baseline (owner_prefix, source_id, stream, local_sequence) VALUES (@owner, @source, @stream, @sequence)",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .set("stream", stream.to_owned())
            .set("sequence", sequence)
            .build(),
    )?;
    Ok(sequence)
}

fn local_baseline(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    stream: &str,
) -> Result<i64, Error> {
    let rows = tx.execute(
        "SELECT local_sequence FROM history_sync_local_baseline WHERE owner_prefix = @owner AND source_id = @source AND stream = @stream LIMIT 1",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .set("stream", stream.to_owned())
            .build(),
    )?;
    Ok(rows.first().and_then(|row| row[0].as_i64()).unwrap_or(0))
}

fn save_local_anchor(
    tx: &mut DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    stream: &str,
    user_id: &str,
    sequence: i64,
) -> Result<(), Error> {
    tx.execute_non_query(
        "INSERT INTO history_sync_local_anchor (owner_prefix, source_id, stream, user_id, local_sequence) VALUES (@owner, @source, @stream, @user, @sequence) ON CONFLICT(owner_prefix, source_id, stream, user_id) DO UPDATE SET local_sequence = MAX(local_sequence, excluded.local_sequence)",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .set("stream", stream.to_owned())
            .set("user", user_id.to_owned())
            .set("sequence", sequence)
            .build(),
    )?;
    Ok(())
}

fn load_local_candidates(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    stream_name: &str,
    user_id: &str,
    anchor: i64,
) -> Result<Vec<LocalCandidate>, Error> {
    let stream = stream_spec(stream_name)?;
    let table = format!("{prefix}_{stream_name}");
    let json_pairs = stream
        .columns
        .iter()
        .map(|column| format!("'{column}', t.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let current_event = format!("json_object({json_pairs})");
    let lower = anchor;
    let identity_columns = stream
        .columns
        .iter()
        .filter(|column| {
            !matches!(
                **column,
                "display_name"
                    | "world_name"
                    | "avatar_name"
                    | "time"
                    | "group_name"
                    | "friend_number"
            )
        })
        .map(|column| format!("t.{column} IS json_extract(j.event_json, '$.{column}')"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let rows = tx.execute(
        &format!(
            "SELECT j.sequence, j.source_row_id, {current_event} FROM history_sync_journal j JOIN {table} t ON t.id = j.source_row_id AND {identity_columns} WHERE j.owner_prefix = @owner AND j.stream = @stream AND j.sequence > @after AND t.user_id = @user AND NOT EXISTS (SELECT 1 FROM history_sync_imported_journal i WHERE i.owner_prefix = @owner AND i.source_id = @source AND i.stream = @stream AND i.sequence = j.sequence) AND NOT EXISTS (SELECT 1 FROM history_sync_remote_map m WHERE m.owner_prefix = @owner AND m.source_id = @source AND m.stream = @stream AND m.local_sequence = j.sequence) ORDER BY j.sequence DESC LIMIT @limit"
        ),
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .set("stream", stream_name.to_owned())
            .set("user", user_id.to_owned())
            .set("after", lower)
            .set("limit", MAX_LOCAL_CANDIDATES)
            .build(),
    )?;
    let mut rows = rows;
    rows.reverse();
    rows.into_iter()
        .map(|row| {
            let sequence = row[0]
                .as_i64()
                .ok_or_else(|| Error::Database("Invalid local history sequence.".into()))?;
            let row_id = row[1]
                .as_i64()
                .ok_or_else(|| Error::Database("Invalid local history row id.".into()))?;
            let event_json = row[2]
                .as_str()
                .ok_or_else(|| Error::Database("Invalid local history event.".into()))?;
            let event: Value = serde_json::from_str(event_json).map_err(|error| {
                Error::Database(format!("Invalid local history event: {error}"))
            })?;
            let record = HistorySyncRecord {
                stream: stream_name.to_owned(),
                source_row_id: sequence,
                event: event.clone(),
            };
            Ok(LocalCandidate {
                sequence,
                row_id,
                signature: semantic_signature(&record)?,
            })
        })
        .collect()
}

fn semantic_signature(record: &HistorySyncRecord) -> Result<String, Error> {
    let event = &record.event;
    let value = match record.stream.as_str() {
        "feed_gps" => json!([
            record.stream.as_str(),
            event.get("user_id"),
            event.get("previous_location"),
            event.get("location"),
        ]),
        "feed_online_offline" => json!([
            record.stream.as_str(),
            event.get("user_id"),
            event.get("type"),
        ]),
        "feed_status" => json!([
            record.stream.as_str(),
            event.get("user_id"),
            event.get("previous_status"),
            event.get("status"),
            event.get("previous_status_description"),
            event.get("status_description"),
        ]),
        "feed_bio" => json!([
            record.stream.as_str(),
            event.get("user_id"),
            event.get("previous_bio"),
            event.get("bio"),
        ]),
        "feed_avatar" => json!([
            record.stream.as_str(),
            event.get("user_id"),
            event.get("owner_id"),
            event.get("previous_current_avatar_image_url"),
            event.get("current_avatar_image_url"),
            event.get("previous_current_avatar_thumbnail_image_url"),
            event.get("current_avatar_thumbnail_image_url"),
        ]),
        "friend_log_history" => match event.get("type").and_then(Value::as_str) {
            Some("Friend" | "Unfriend") => json!([
                record.stream.as_str(),
                event.get("user_id"),
                event.get("type"),
            ]),
            Some("DisplayName") => json!([
                record.stream.as_str(),
                event.get("user_id"),
                event.get("type"),
                event.get("previous_display_name"),
                event.get("display_name"),
            ]),
            Some("TrustLevel") => json!([
                record.stream.as_str(),
                event.get("user_id"),
                event.get("type"),
                event.get("previous_trust_level"),
                event.get("trust_level"),
            ]),
            _ => json!([
                record.stream.as_str(),
                event.get("user_id"),
                event.get("type"),
                event.get("previous_display_name"),
                event.get("display_name"),
                event.get("previous_trust_level"),
                event.get("trust_level"),
            ]),
        },
        other => {
            return Err(Error::Database(format!(
                "Unsupported history sync stream: {other}"
            )));
        }
    };
    serde_json::to_string(&value)
        .map_err(|error| Error::Database(format!("Could not encode history transition: {error}")))
}

fn ordered_alignment(remote: &[&str], local: &[&str]) -> Vec<(usize, usize)> {
    let rows = remote.len() + 1;
    let columns = local.len() + 1;
    let mut lengths = vec![0_u16; rows.saturating_mul(columns)];
    for i in (0..remote.len()).rev() {
        for j in (0..local.len()).rev() {
            let value = if remote[i] == local[j] {
                1 + lengths[(i + 1) * columns + j + 1]
            } else {
                lengths[(i + 1) * columns + j].max(lengths[i * columns + j + 1])
            };
            lengths[i * columns + j] = value;
        }
    }
    let mut matches = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < remote.len() && j < local.len() {
        if remote[i] == local[j]
            && lengths[i * columns + j] == 1 + lengths[(i + 1) * columns + j + 1]
        {
            matches.push((i, j));
            i += 1;
            j += 1;
        } else if lengths[(i + 1) * columns + j] > lengths[i * columns + j + 1] {
            i += 1;
        } else {
            // Stable tie breaking keeps the remote order and advances through
            // the local suffix so repeated transitions remain one-to-one.
            j += 1;
        }
    }
    matches
}

fn insert_history_record(
    tx: &mut DatabaseWriteTransaction<'_>,
    prefix: &str,
    record: &HistorySyncRecord,
) -> Result<i64, Error> {
    let stream = stream_spec(&record.stream)?;
    let table = format!("{prefix}_{}", stream.name);
    let columns = stream.columns.join(", ");
    let placeholders = stream
        .columns
        .iter()
        .map(|column| format!("@event_{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut args = ParamsBuilder::new();
    for column in stream.columns {
        args = args.set(
            &format!("event_{column}"),
            record.event.get(*column).cloned().unwrap_or(Value::Null),
        );
    }
    tx.execute_non_query(
        &format!("INSERT INTO {table} ({columns}) VALUES ({placeholders})"),
        &args.build(),
    )?;
    let rows = tx.execute("SELECT last_insert_rowid()", &Default::default())?;
    rows.first()
        .and_then(|row| row[0].as_i64())
        .ok_or_else(|| Error::Database("Could not read imported history row id.".into()))
}

fn find_exact_row_id(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    stream_name: &str,
    record: &HistorySyncRecord,
) -> Result<Option<i64>, Error> {
    let stream = stream_spec(stream_name)?;
    let table = format!("{prefix}_{stream_name}");
    let conditions = stream
        .columns
        .iter()
        .map(|column| format!("{column} IS @event_{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut args = ParamsBuilder::new();
    for column in stream.columns {
        args = args.set(
            &format!("event_{column}"),
            record.event.get(*column).cloned().unwrap_or(Value::Null),
        );
    }
    let rows = tx.execute(
        &format!("SELECT id FROM {table} WHERE {conditions} ORDER BY id DESC LIMIT 1"),
        &args.build(),
    )?;
    match rows.first() {
        None => Ok(None),
        Some(row) => row[0]
            .as_i64()
            .map(Some)
            .ok_or_else(|| Error::Database("Invalid exact history row id.".into())),
    }
}

fn local_journal_sequence(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    stream: &str,
    row_id: i64,
) -> Result<i64, Error> {
    let rows = tx.execute(
        "SELECT sequence FROM history_sync_journal WHERE owner_prefix = @owner AND stream = @stream AND source_row_id = @row_id ORDER BY sequence DESC LIMIT 1",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("stream", stream.to_owned())
            .set("row_id", row_id)
            .build(),
    )?;
    rows.first()
        .and_then(|row| row[0].as_i64())
        .ok_or_else(|| Error::Database("Imported history was not journaled.".into()))
}

fn insert_remote_mapping(
    tx: &mut DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    event: &RemoteEvent,
    local_sequence: i64,
    local_row_id: i64,
    pending_local: bool,
) -> Result<(), Error> {
    let event_json = serde_json::to_string(&event.record.event)
        .map_err(|error| Error::Database(format!("Could not encode remote event: {error}")))?;
    tx.execute_non_query(
        "INSERT INTO history_sync_remote_map (owner_prefix, source_id, stream, source_sequence, user_id, local_sequence, local_row_id, semantic_key, event_json, pending_local) VALUES (@owner, @source, @stream, @source_sequence, @user, @local_sequence, @local_row_id, @semantic_key, @event_json, @pending_local)",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .set("stream", event.record.stream.clone())
            .set("source_sequence", event.source_sequence)
            .set("user", event.user_id.clone())
            .set("local_sequence", local_sequence)
            .set("local_row_id", local_row_id)
            .set("semantic_key", event.signature.clone())
            .set("event_json", event_json)
            .set("pending_local", if pending_local { 1 } else { 0 })
            .build(),
    )?;
    Ok(())
}

fn validate_page_cursors(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    records: &[HistorySyncRecord],
    cursors: &[HistorySyncStreamCursor],
) -> Result<(), Error> {
    for cursor in cursors {
        let previous = tx.execute(
            "SELECT row_id FROM history_sync_reconciled_checkpoint WHERE owner_prefix = @owner AND source_id = @source AND stream = @stream",
            &ParamsBuilder::new()
                .set("owner", prefix.to_owned())
                .set("source", source_id.to_owned())
                .set("stream", cursor.stream.clone())
                .build(),
        )?;
        let current = previous
            .first()
            .and_then(|row| row[0].as_i64())
            .unwrap_or(0);
        let page_max = records
            .iter()
            .filter(|record| record.stream == cursor.stream)
            .map(|record| record.source_row_id)
            .max()
            .unwrap_or(0);
        if cursor.row_id > current && cursor.row_id > page_max {
            return Err(Error::Database(format!(
                "History sync cursor for {} is invalid for this page.",
                cursor.stream
            )));
        }
    }
    Ok(())
}

fn save_page_cursors(
    tx: &mut DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
    cursors: &[HistorySyncStreamCursor],
) -> Result<(), Error> {
    for cursor in cursors {
        tx.execute_non_query(
            "INSERT INTO history_sync_reconciled_checkpoint (owner_prefix, source_id, stream, row_id) VALUES (@owner, @source, @stream, @row_id) ON CONFLICT(owner_prefix, source_id, stream) DO UPDATE SET row_id = MAX(row_id, excluded.row_id)",
            &ParamsBuilder::new()
                .set("owner", prefix.to_owned())
                .set("source", source_id.to_owned())
                .set("stream", cursor.stream.clone())
                .set("row_id", cursor.row_id)
                .build(),
        )?;
    }
    Ok(())
}

fn load_page_cursors(
    tx: &DatabaseWriteTransaction<'_>,
    prefix: &str,
    source_id: &str,
) -> Result<Vec<HistorySyncStreamCursor>, Error> {
    let rows = tx.execute(
        "SELECT stream, row_id FROM history_sync_reconciled_checkpoint WHERE owner_prefix = @owner AND source_id = @source ORDER BY stream",
        &ParamsBuilder::new()
            .set("owner", prefix.to_owned())
            .set("source", source_id.to_owned())
            .build(),
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
