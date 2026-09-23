use std::{collections::HashMap, io::Write};

use aw_models::{Bucket, Event};
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde::{
    ser::{Error, SerializeMap, SerializeSeq},
    Serialize, Serializer,
};

use crate::datastore::{parse_event_row, prefer_endtime_index};
use crate::DatastoreError;

struct EventRows<'a> {
    conn: &'a Connection,
    bucket: &'a Bucket,
}

impl Serialize for EventRows<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT id, starttime, endtime, data
             FROM events INDEXED BY events_bucketrow_starttime_endtime_index
             WHERE bucketrow = ?1 AND endtime >= 0 AND starttime <= ?2
             ORDER BY starttime DESC, endtime ASC, id ASC",
            )
            .map_err(S::Error::custom)?;
        let mut rows = stmt
            .query(rusqlite::params![self.bucket.bid.unwrap(), i64::MAX])
            .map_err(S::Error::custom)?;
        let mut seq = serializer.serialize_seq(None)?;
        while let Some(row) = rows.next().map_err(S::Error::custom)? {
            // Match get_events' default range, clipping and corrupt-row policy.
            match crate::datastore::parse_event_row(row, Some((0, i64::MAX))) {
                Ok(event) => seq.serialize_element(&event)?,
                Err(err) => warn!("Corrupt event in bucket {}: {}", self.bucket.id, err),
            }
        }
        seq.end()
    }
}

struct ExportBucket<'a> {
    conn: &'a Connection,
    bucket: &'a Bucket,
}

impl Serialize for ExportBucket<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Reuse Bucket's field names and serialization rules, replacing only
        // events. This allocation contains bucket metadata, never event rows.
        let metadata = serde_json::to_value(self.bucket).map_err(S::Error::custom)?;
        let metadata = metadata
            .as_object()
            .ok_or_else(|| S::Error::custom("invalid bucket metadata"))?;
        let mut map = serializer.serialize_map(Some(metadata.len()))?;
        for (key, value) in metadata {
            if key != "events" {
                map.serialize_entry(key, value)?;
            }
        }
        map.serialize_entry(
            "events",
            &EventRows {
                conn: self.conn,
                bucket: self.bucket,
            },
        )?;
        map.end()
    }
}

struct ExportBuckets<'a> {
    conn: &'a Connection,
    buckets: &'a HashMap<String, Bucket>,
    selected: Option<&'a str>,
}

impl Serialize for ExportBuckets<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (id, bucket) in self.buckets {
            if self.selected.is_none() || self.selected == Some(id.as_str()) {
                map.serialize_entry(
                    id,
                    &ExportBucket {
                        conn: self.conn,
                        bucket,
                    },
                )?;
            }
        }
        map.end()
    }
}

pub(crate) fn write_export(
    conn: &Connection,
    buckets: &HashMap<String, Bucket>,
    selected: Option<&str>,
    writer: impl Write,
) -> Result<(), DatastoreError> {
    if let Some(id) = selected {
        if !buckets.contains_key(id) {
            return Err(DatastoreError::NoSuchBucket(id.to_owned()));
        }
    }
    #[derive(Serialize)]
    struct Export<'a> {
        buckets: ExportBuckets<'a>,
    }
    serde_json::to_writer(
        writer,
        &Export {
            buckets: ExportBuckets {
                conn,
                buckets,
                selected,
            },
        },
    )
    .map_err(|err| DatastoreError::InternalError(format!("Failed to write export: {err}")))
}

fn csv_io_err(err: std::io::Error) -> DatastoreError {
    DatastoreError::InternalError(format!("Failed to write CSV export: {err}"))
}

/// Prefix spreadsheet-formula starters so Excel/Sheets will not execute them.
fn neutralize_formula(s: &str) -> String {
    match s.chars().next() {
        Some('=' | '+' | '-' | '@' | '\t' | '\r') => format!("'{s}"),
        _ => s.to_owned(),
    }
}

/// RFC-4180 field escaping, with formula neutralization applied first.
fn csv_escape(s: &str) -> String {
    let s = neutralize_formula(s);
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s
    }
}

/// Exact fractional-second duration, matching the JSON nanosecond contract
/// without going through `num_milliseconds()` (which truncates sub-ms).
fn duration_csv(duration: &chrono::Duration) -> String {
    let ns = duration.num_nanoseconds().unwrap_or(0);
    let sign = if ns < 0 { "-" } else { "" };
    let ns = ns.unsigned_abs();
    format!("{sign}{}.{:09}", ns / 1_000_000_000, ns % 1_000_000_000)
}

fn event_field_value(event: &Event, key: &str) -> String {
    match event.data.get(key) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string(),
        None => String::new(),
    }
}

fn write_csv_record(
    writer: &mut impl Write,
    fields: impl IntoIterator<Item = String>,
) -> Result<(), DatastoreError> {
    let mut first = true;
    for field in fields {
        if !first {
            writer.write_all(b",").map_err(csv_io_err)?;
        }
        first = false;
        writer
            .write_all(csv_escape(&field).as_bytes())
            .map_err(csv_io_err)?;
    }
    writer.write_all(b"\n").map_err(csv_io_err)
}

fn write_csv_header(writer: &mut impl Write, data_keys: &[String]) -> Result<(), DatastoreError> {
    let mut fields = vec![
        "id".to_string(),
        "timestamp".to_string(),
        "duration".to_string(),
    ];
    fields.extend(data_keys.iter().cloned());
    write_csv_record(writer, fields)
}

fn write_csv_event(
    writer: &mut impl Write,
    event: &Event,
    data_keys: &[String],
) -> Result<(), DatastoreError> {
    let mut fields = vec![
        event.id.map(|i| i.to_string()).unwrap_or_default(),
        event.timestamp.to_rfc3339(),
        duration_csv(&event.duration),
    ];
    for key in data_keys {
        fields.push(event_field_value(event, key));
    }
    write_csv_record(writer, fields)
}

/// Write an already-fetched event slice as RFC-4180 CSV.
///
/// Unlike `write_events_csv`, this does not need a database connection —
/// call it from a background thread after fetching events via `get_events`
/// so the datastore worker is free during the (potentially long) serialization.
///
/// Columns: `id`, `timestamp`, `duration`, then all keys from the first
/// event's data map.
pub fn write_csv_from_events(
    events: &[aw_models::Event],
    mut writer: impl Write,
) -> Result<(), DatastoreError> {
    let data_keys: Vec<String> = events
        .first()
        .map(|e| e.data.keys().cloned().collect())
        .unwrap_or_default();
    write_csv_header(&mut writer, &data_keys)?;
    for event in events {
        write_csv_event(&mut writer, event, &data_keys)?;
    }
    Ok(())
}

/// Stream events for one bucket as RFC-4180 CSV, writing one row at a time.
///
/// Columns: `id`, `timestamp`, `duration`, then all keys from the first
/// valid event's `data` map (same schema the webui uses for client-side CSV).
/// Query filters, clipping, and corrupt-row skipping match `get_events`.
pub(crate) fn write_events_csv(
    conn: &Connection,
    buckets: &HashMap<String, Bucket>,
    bucket_id: &str,
    starttime_opt: Option<DateTime<Utc>>,
    endtime_opt: Option<DateTime<Utc>>,
    limit_opt: Option<u64>,
    mut writer: impl Write,
) -> Result<(), DatastoreError> {
    let bucket = match buckets.get(bucket_id) {
        Some(bucket) => bucket,
        None => return Err(DatastoreError::NoSuchBucket(bucket_id.to_owned())),
    };

    let starttime_filter_ns: i64 = match starttime_opt {
        Some(dt) => dt.timestamp_nanos_opt().unwrap(),
        None => 0,
    };
    let endtime_filter_ns: i64 = match endtime_opt {
        Some(dt) => dt.timestamp_nanos_opt().unwrap(),
        None => i64::MAX,
    };
    if starttime_filter_ns > endtime_filter_ns {
        warn!("Starttime in event query was lower than endtime!");
        return write_csv_header(&mut writer, &[]);
    }
    let limit = match limit_opt {
        Some(l) => l as i64,
        None => -1,
    };

    let sql = if prefer_endtime_index(bucket, starttime_filter_ns, endtime_filter_ns, limit_opt) {
        "SELECT id, starttime, endtime, data
             FROM events INDEXED BY events_bucketrow_endtime_starttime_index
             WHERE bucketrow = ?1 AND endtime >= ?2 AND starttime <= ?3
             ORDER BY starttime DESC, endtime ASC, id ASC LIMIT ?4"
    } else {
        "SELECT id, starttime, endtime, data
             FROM events INDEXED BY events_bucketrow_starttime_endtime_index
             WHERE bucketrow = ?1 AND endtime >= ?2 AND starttime <= ?3
             ORDER BY starttime DESC, endtime ASC, id ASC LIMIT ?4"
    };
    let mut stmt = conn.prepare_cached(sql).map_err(|err| {
        DatastoreError::InternalError(format!("Failed to prepare CSV export SQL: {err}"))
    })?;
    let mut rows = stmt
        .query(rusqlite::params![
            bucket.bid.unwrap(),
            starttime_filter_ns,
            endtime_filter_ns,
            limit,
        ])
        .map_err(|err| {
            DatastoreError::InternalError(format!("Failed to query CSV export SQL: {err}"))
        })?;

    let clip = Some((starttime_filter_ns, endtime_filter_ns));
    let mut data_keys: Option<Vec<String>> = None;
    while let Some(row) = rows.next().map_err(|err| {
        DatastoreError::InternalError(format!("Failed to read CSV export row: {err}"))
    })? {
        let event = match parse_event_row(row, clip) {
            Ok(event) => event,
            Err(err) => {
                warn!("Corrupt event in bucket {}: {}", bucket_id, err);
                continue;
            }
        };
        if data_keys.is_none() {
            let keys: Vec<String> = event.data.keys().cloned().collect();
            write_csv_header(&mut writer, &keys)?;
            data_keys = Some(keys);
        }
        write_csv_event(&mut writer, &event, data_keys.as_ref().unwrap())?;
    }
    if data_keys.is_none() {
        write_csv_header(&mut writer, &[])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DatastoreInstance;
    use aw_models::{BucketMetadata, BucketsExport, Event, TryVec};
    use chrono::{DateTime, Duration};

    fn setup() -> (Connection, DatastoreInstance) {
        let conn = Connection::open_in_memory().unwrap();
        let mut ds = DatastoreInstance::new(&conn, true).unwrap();
        for id in ["populated", "empty"] {
            ds.create_bucket(
                &conn,
                Bucket {
                    bid: None,
                    id: id.into(),
                    _type: "test".into(),
                    client: "test".into(),
                    hostname: "host".into(),
                    created: None,
                    data: Default::default(),
                    metadata: BucketMetadata::default(),
                    events: None,
                    last_updated: None,
                },
            )
            .unwrap();
        }
        let events = [(-10, 20), (5, 20), (5, 10), (5, 10), (30, 0)]
            .into_iter()
            .map(|(start, duration)| {
                Event::new(
                    DateTime::from_timestamp(start, 0).unwrap(),
                    Duration::seconds(duration),
                    serde_json::from_value(
                        serde_json::json!({"text": "quotes \" and unicode ☀", "nested": [1, true]}),
                    )
                    .unwrap(),
                )
            })
            .collect();
        ds.insert_events(&conn, "populated", events).unwrap();
        conn.execute("INSERT INTO events(bucketrow,starttime,endtime,data) VALUES(1,6000000000,7000000000,'invalid json')", []).unwrap();
        (conn, ds)
    }

    #[test]
    fn streamed_json_matches_materialized_exports() {
        let (conn, mut ds) = setup();
        for selected in [None, Some("populated"), Some("empty")] {
            let mut buckets = ds.get_buckets();
            buckets.retain(|id, _| selected.is_none() || selected == Some(id.as_str()));
            for (id, bucket) in &mut buckets {
                bucket.events = Some(TryVec::new(
                    ds.get_events(&conn, id, None, None, None).unwrap(),
                ));
            }
            let expected = serde_json::to_value(BucketsExport { buckets }).unwrap();
            let mut output = Vec::new();
            ds.write_export(&conn, selected, &mut output).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&output).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn missing_bucket_and_writer_failures_propagate() {
        let (conn, ds) = setup();
        let mut output = Vec::new();
        assert!(matches!(
            ds.write_export(&conn, Some("missing"), &mut output),
            Err(DatastoreError::NoSuchBucket(_))
        ));
        assert!(output.is_empty());
        struct FailingWriter(usize);
        impl Write for FailingWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.0 == 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, "disk full"));
                }
                let n = bytes.len().min(self.0);
                self.0 -= n;
                Ok(n)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        assert!(matches!(
            ds.write_export(&conn, None, FailingWriter(500)),
            Err(DatastoreError::InternalError(_))
        ));
    }

    #[test]
    fn csv_duration_preserves_sub_millisecond_nanos() {
        assert_eq!(
            duration_csv(&Duration::nanoseconds(1_500_000)),
            "0.001500000"
        );
        assert_eq!(duration_csv(&Duration::milliseconds(1)), "0.001000000");
        assert_eq!(duration_csv(&Duration::seconds(0)), "0.000000000");
    }

    #[test]
    fn csv_escape_neutralizes_formula_prefixes_and_quotes_rfc4180() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("=1+1"), "'=1+1");
        assert_eq!(csv_escape("+cmd"), "'+cmd");
        assert_eq!(csv_escape("-1+1"), "'-1+1");
        assert_eq!(csv_escape("@SUM(A1)"), "'@SUM(A1)");
        assert_eq!(
            csv_escape("A \"quoted\" title"),
            "\"A \"\"quoted\"\" title\""
        );
        assert_eq!(csv_escape("=1,2"), "\"'=1,2\"");
    }

    #[test]
    fn streamed_csv_preserves_precision_neutralizes_formulas_and_rejects_missing() {
        let (conn, mut ds) = setup();
        let event = Event::new(
            DateTime::from_timestamp(0, 0).unwrap(),
            Duration::nanoseconds(1_500_000),
            serde_json::from_value(serde_json::json!({
                "app": "firefox",
                "title": "=cmd|calc"
            }))
            .unwrap(),
        );
        ds.insert_events(&conn, "empty", vec![event]).unwrap();

        let mut output = Vec::new();
        ds.write_events_csv(&conn, "empty", None, None, None, &mut output)
            .unwrap();
        let csv = String::from_utf8(output).unwrap();
        assert!(csv.starts_with("id,timestamp,duration,"), "{csv}");
        assert!(csv.contains("0.001500000"), "duration: {csv}");
        assert!(csv.contains("'=cmd|calc"), "formula: {csv}");

        let mut missing = Vec::new();
        assert!(matches!(
            ds.write_events_csv(&conn, "missing", None, None, None, &mut missing),
            Err(DatastoreError::NoSuchBucket(_))
        ));
        assert!(missing.is_empty());
    }

    #[test]
    fn streamed_csv_matches_get_events_row_count() {
        let (conn, mut ds) = setup();
        let events = ds.get_events(&conn, "populated", None, None, None).unwrap();
        let mut output = Vec::new();
        ds.write_events_csv(&conn, "populated", None, None, None, &mut output)
            .unwrap();
        let csv = String::from_utf8(output).unwrap();
        assert_eq!(csv.lines().count(), events.len() + 1, "{csv}");
        assert!(csv.contains("\"quotes \"\" and unicode ☀\""), "{csv}");
    }
}
