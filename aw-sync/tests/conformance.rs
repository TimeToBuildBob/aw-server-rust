//! Property-based conformance harness for aw-sync.
//!
//! One invariant: after a push and a pull, the destination's synced copy of a
//! bucket holds exactly the source's events by tuple identity
//! `(timestamp, duration, data)`. Row ids are not identity; aw-sync must never
//! rely on them across datastores.
//!
//! The operation model covers the paths where aw-sync bugs have historically
//! arrived as a class rather than one at a time (ActivityWatch/aw-server-rust
//! #790, #793, #798 all came out of one PR): heartbeat extension of the
//! trailing open event, edits inside the reconcile lookback window, deletes,
//! and interleaved push/pull passes so a stale resume cursor is exercised.
//!
//! The three filed bugs are pinned as named regression cases below so the
//! harness demonstrably covers them; proptest shrinks any new failure to a
//! minimal sequence.
use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use proptest::prelude::*;
use serde_json::json;

use aw_datastore::Datastore;
use aw_models::{Bucket, Event};
use aw_sync::{sync_datastores, SyncSpec};

const SRC_BUCKET: &str = "aw-watcher-window_HOSTA";
const SRC_HOST: &str = "HOSTA";
const DEST_BUCKET: &str = "aw-watcher-window_HOSTA-synced-from-hosta";

fn base_ts() -> DateTime<Utc> {
    "2024-01-01T00:00:00Z".parse().unwrap()
}

fn bucket(id: &str, hostname: &str) -> Bucket {
    serde_json::from_str(&format!(
        r#"{{"id": "{id}", "type": "currentwindow", "hostname": "{hostname}", "client": "test"}}"#
    ))
    .unwrap()
}

fn event(ts: DateTime<Utc>, secs: i64, title: &str) -> Event {
    let mut data = serde_json::Map::new();
    data.insert("app".to_string(), json!("app"));
    data.insert("title".to_string(), json!(title));
    Event {
        id: None,
        timestamp: ts,
        duration: Duration::seconds(secs),
        data,
    }
}

/// Tuple identity of every event in a bucket, independent of row ids.
fn fingerprint(ds: &Datastore, bucket_id: &str) -> BTreeMap<(DateTime<Utc>, i64, String), usize> {
    let mut out = BTreeMap::new();
    if let Ok(events) = ds.get_events(bucket_id, None, None, None) {
        for e in events {
            let key = (
                e.timestamp,
                e.duration.num_milliseconds(),
                serde_json::to_string(&e.data).unwrap(),
            );
            *out.entry(key).or_insert(0) += 1;
        }
    }
    out
}

#[derive(Debug, Clone)]
enum Op {
    /// Append an event `gap` minutes after the previous one.
    Insert {
        gap: u8,
        secs: u8,
        title: u8,
    },
    /// Extend the trailing event via the heartbeat path (same data, later end).
    Heartbeat {
        extend_secs: u8,
    },
    /// Delete+insert at the same timestamp with a new title (the WebUI/Android edit path).
    Edit {
        idx: u8,
        title: u8,
    },
    Delete {
        idx: u8,
    },
    /// Insert a second, distinct event at the previous event's timestamp
    /// (two watchers can legitimately emit at the same instant).
    InsertSameTs {
        title: u8,
    },
    /// Push source → export, then pull export → destination.
    Sync,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (1u8..30, 0u8..120, 0u8..4).prop_map(|(gap, secs, title)| Op::Insert { gap, secs, title }),
        2 => (1u8..120).prop_map(|extend_secs| Op::Heartbeat { extend_secs }),
        2 => (0u8..8, 0u8..4).prop_map(|(idx, title)| Op::Edit { idx, title }),
        1 => (0u8..8).prop_map(|idx| Op::Delete { idx }),
        1 => (0u8..4).prop_map(|title| Op::InsertSameTs { title }),
        3 => Just(Op::Sync),
    ]
}

struct World {
    src: Datastore,
    export: Datastore,
    dest: Datastore,
    spec: SyncSpec,
    next_ts: DateTime<Utc>,
}

impl World {
    fn new() -> Self {
        let src = Datastore::new_in_memory(false);
        src.create_bucket(&bucket(SRC_BUCKET, SRC_HOST)).unwrap();
        World {
            src,
            export: Datastore::new_in_memory(false),
            dest: Datastore::new_in_memory(false),
            spec: SyncSpec::default(),
            next_ts: base_ts(),
        }
    }

    fn source_events(&self) -> Vec<Event> {
        let mut events = self.src.get_events(SRC_BUCKET, None, None, None).unwrap();
        events.sort_by_key(|e| e.timestamp);
        events
    }

    fn apply(&mut self, op: &Op) {
        match op {
            Op::Insert { gap, secs, title } => {
                let ts = self.next_ts;
                self.src
                    .insert_events(SRC_BUCKET, &[event(ts, *secs as i64, &format!("t{title}"))])
                    .unwrap();
                self.next_ts = ts + Duration::minutes(*gap as i64);
            }
            Op::Heartbeat { extend_secs } => {
                if let Some(last) = self.source_events().last().cloned() {
                    let mut hb = last.clone();
                    hb.id = None;
                    hb.timestamp = last.timestamp + last.duration;
                    hb.duration = Duration::seconds(*extend_secs as i64);
                    // pulsetime large enough that the heartbeat always merges.
                    let _ = self.src.heartbeat(SRC_BUCKET, hb, 1e9);
                }
            }
            Op::Edit { idx, title } => {
                let events = self.source_events();
                if events.is_empty() {
                    return;
                }
                let target = &events[*idx as usize % events.len()];
                let id = target.id.expect("source event id");
                self.src.delete_events_by_id(SRC_BUCKET, vec![id]).unwrap();
                let mut data = target.data.clone();
                data.insert("title".to_string(), json!(format!("e{title}")));
                self.src
                    .insert_events(
                        SRC_BUCKET,
                        &[Event {
                            id: None,
                            timestamp: target.timestamp,
                            duration: target.duration,
                            data,
                        }],
                    )
                    .unwrap();
            }
            Op::Delete { idx } => {
                let events = self.source_events();
                if events.is_empty() {
                    return;
                }
                let id = events[*idx as usize % events.len()].id.unwrap();
                self.src.delete_events_by_id(SRC_BUCKET, vec![id]).unwrap();
            }
            Op::InsertSameTs { title } => {
                if let Some(last) = self.source_events().last().cloned() {
                    self.src
                        .insert_events(
                            SRC_BUCKET,
                            &[event(last.timestamp, 3, &format!("s{title}"))],
                        )
                        .unwrap();
                }
            }
            Op::Sync => self.sync(),
        }
        self.src.force_commit().unwrap();
    }

    fn sync(&mut self) {
        sync_datastores(&self.src, &self.export, true, Some("device-A"), &self.spec).unwrap();
        sync_datastores(&self.export, &self.dest, false, None, &self.spec).unwrap();
    }

    /// The invariant: destination copy == source by tuple identity.
    fn check(&self) -> Result<(), String> {
        let want = fingerprint(&self.src, SRC_BUCKET);
        let got = fingerprint(&self.dest, DEST_BUCKET);
        if want == got {
            return Ok(());
        }
        let missing: Vec<_> = want.keys().filter(|k| !got.contains_key(*k)).collect();
        let extra: Vec<_> = got.keys().filter(|k| !want.contains_key(*k)).collect();
        Err(format!(
            "destination diverged from source\n  missing in dest: {missing:#?}\n  extra in dest: {extra:#?}"
        ))
    }
}

fn run(ops: &[Op]) -> Result<(), String> {
    let mut w = World::new();
    for op in ops {
        w.apply(op);
    }
    w.sync();
    w.check()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, max_shrink_iters: 2000, ..ProptestConfig::default() })]

    /// After the final push+pull, dest == source by tuple identity for any
    /// interleaving of inserts, heartbeats, edits, deletes and sync passes.
    #[test]
    fn dest_matches_source_by_tuple_identity(ops in prop::collection::vec(op_strategy(), 1..12)) {
        prop_assert!(run(&ops).is_ok(), "{}", run(&ops).unwrap_err());
    }
}

// ---- Named regression seeds (the harness must cover the filed bug class) ----

/// ActivityWatch/aw-server-rust#790: a heartbeat that extends the trailing
/// open event after a sync pass must reach the destination on the next pass.
#[test]
fn seed_790_heartbeat_extends_trailing_event_after_sync() {
    let ops = [
        Op::Insert {
            gap: 5,
            secs: 10,
            title: 0,
        },
        Op::Sync,
        Op::Heartbeat { extend_secs: 30 },
    ];
    run(&ops).unwrap();
}

/// ActivityWatch/aw-server-rust#793: an event inserted *before* the resume
/// cursor (a backfill) after a sync pass must still be picked up.
#[test]
fn seed_793_backfill_behind_cursor_is_recovered() {
    let mut w = World::new();
    w.apply(&Op::Insert {
        gap: 10,
        secs: 5,
        title: 0,
    });
    w.apply(&Op::Insert {
        gap: 10,
        secs: 5,
        title: 1,
    });
    w.sync();
    // Backfill between the two synced events, behind the destination's cursor.
    let ts = base_ts() + Duration::minutes(5);
    w.src
        .insert_events(SRC_BUCKET, &[event(ts, 5, "backfill")])
        .unwrap();
    w.src.force_commit().unwrap();
    w.sync();
    w.check().unwrap();
}

/// ActivityWatch/aw-server-rust#798: a second source event at the same
/// timestamp with different data must not get the earlier destination copy
/// deleted as "stale"; identity is the (timestamp, duration, data) tuple.
#[test]
fn seed_798_same_timestamp_distinct_event_is_not_stale() {
    let ops = [
        Op::Insert {
            gap: 5,
            secs: 10,
            title: 0,
        },
        Op::Sync,
        Op::InsertSameTs { title: 1 },
    ];
    run(&ops).unwrap();
}

/// Shrunk by proptest on master (session 1e11): editing a duration-0 event
/// after it was synced leaves the pre-edit row in the destination.
#[test]
fn seed_shrunk_zero_duration_edit_leaves_stale_row() {
    let ops = [
        Op::Insert {
            gap: 1,
            secs: 0,
            title: 0,
        },
        Op::Sync,
        Op::Edit { idx: 0, title: 0 },
    ];
    run(&ops).unwrap();
}
