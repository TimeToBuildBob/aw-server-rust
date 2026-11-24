//! Bug-hunt (error handling / data loss lens). Each test FAILS on master ea6f09c.

use std::cell::Cell;
use std::collections::HashMap;

use chrono::{DateTime, Duration, TimeZone, Utc};

use aw_datastore::{Datastore, DatastoreError};
use aw_models::{Bucket, Event};
use aw_sync::{AccessMethod, SyncSpec};

/// Delegates to a real Datastore, but `insert_events` can be made to fail on
/// selected calls (transient SQLITE_BUSY / HTTP timeout to aw-server / disk full).
struct Flaky<'a> {
    inner: &'a Datastore,
    insert_calls: Cell<usize>,
    /// 1-based insert_events call numbers that fail.
    fail_insert_calls: Vec<usize>,
    /// Bucket ids whose inserts always fail.
    fail_bucket: Option<String>,
}

impl std::fmt::Debug for Flaky<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Flaky")
    }
}

impl AccessMethod for Flaky<'_> {
    fn get_buckets(&self) -> Result<HashMap<String, Bucket>, String> {
        AccessMethod::get_buckets(self.inner)
    }
    fn get_bucket(&self, bucket_id: &str) -> Result<Bucket, DatastoreError> {
        AccessMethod::get_bucket(self.inner, bucket_id)
    }
    fn create_bucket(&self, bucket: &Bucket) -> Result<(), DatastoreError> {
        AccessMethod::create_bucket(self.inner, bucket)
    }
    fn get_events(
        &self,
        bucket_id: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u64>,
    ) -> Result<Vec<Event>, String> {
        AccessMethod::get_events(self.inner, bucket_id, start, end, limit)
    }
    fn insert_events(&self, bucket_id: &str, events: Vec<Event>) -> Result<(), String> {
        let n = self.insert_calls.get() + 1;
        self.insert_calls.set(n);
        if self.fail_insert_calls.contains(&n) || self.fail_bucket.as_deref() == Some(bucket_id) {
            return Err(format!("injected transient failure on insert #{n}"));
        }
        AccessMethod::insert_events(self.inner, bucket_id, events)
    }
    fn get_event_count(&self, bucket_id: &str) -> Result<i64, String> {
        AccessMethod::get_event_count(self.inner, bucket_id)
    }
    fn heartbeat(&self, bucket_id: &str, event: Event, duration: f64) -> Result<(), String> {
        if self.fail_bucket.as_deref() == Some(bucket_id) {
            return Err("injected transient failure on heartbeat".into());
        }
        AccessMethod::heartbeat(self.inner, bucket_id, event, duration)
    }
    fn delete_events_by_id(&self, bucket_id: &str, event_ids: Vec<i64>) -> Result<(), String> {
        AccessMethod::delete_events_by_id(self.inner, bucket_id, event_ids)
    }
}

fn bucket(id: &str, hostname: &str) -> Bucket {
    serde_json::from_value(serde_json::json!({
        "id": id, "type": "test", "hostname": hostname, "client": "test"
    }))
    .unwrap()
}

/// `n` back-to-back 10s events with distinct data, starting 2020-01-01.
fn fill(ds: &Datastore, bucket_id: &str, n: i64) {
    let t0 = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let events: Vec<Event> = (0..n)
        .map(|i| {
            serde_json::from_value(serde_json::json!({
                "timestamp": (t0 + Duration::seconds(10 * i)).to_rfc3339(),
                "duration": 5.0,
                "data": {"i": i},
            }))
            .unwrap()
        })
        .collect();
    ds.insert_events(bucket_id, &events).unwrap();
    ds.force_commit().unwrap();
}

fn pull(
    src: &dyn AccessMethod,
    dest: &dyn AccessMethod,
) -> Result<Vec<aw_sync::BucketReport>, String> {
    aw_sync::sync_datastores(src, dest, false, None, &SyncSpec::default())
}

/// sync_one writes pages NEWEST-first. If a later (older) page fails, the dest
/// already holds the newest page; the next pass resumes from dest's newest
/// event and never fetches the older range again -> permanent gap.
#[test]
fn partial_multipage_write_permanently_loses_older_events() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    src.create_bucket(&bucket("win", "phone")).unwrap();
    fill(&src, "win", 12_000); // > 2 * BATCH_SIZE (5000)

    // Pass 1: the second page insert hits a transient error.
    let flaky = Flaky {
        inner: &dest,
        insert_calls: Cell::new(0),
        fail_insert_calls: vec![2],
        fail_bucket: None,
    };
    let _ = pull(&src, &flaky); // Err (only bucket failed), logged as "next pass resumes"

    // Pass 2 and 3: everything healthy.
    pull(&src, &dest).expect("healthy pass");
    pull(&src, &dest).expect("healthy pass");

    let dest_id = "win-synced-from-phone";
    let n_src = src.get_event_count("win", None, None).unwrap();
    let n_dest = dest.get_event_count(dest_id, None, None).unwrap();
    assert_eq!(
        n_dest,
        n_src,
        "after a transient failure and two healthy passes, dest is still missing {} events",
        n_src - n_dest
    );
}

/// One bucket failing inside a peer pass returns Ok with that bucket simply
/// absent: there is no failure channel, so pull_from_remotes records the peer
/// as `Imported` and the report/summary/status show a clean pass.
#[test]
fn per_bucket_failure_is_invisible_in_ok_result() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    src.create_bucket(&bucket("afk", "phone")).unwrap();
    src.create_bucket(&bucket("win", "phone")).unwrap();
    fill(&src, "afk", 10);
    fill(&src, "win", 10);

    let flaky = Flaky {
        inner: &dest,
        insert_calls: Cell::new(0),
        fail_insert_calls: vec![],
        fail_bucket: Some("win-synced-from-phone".into()),
    };
    let result = pull(&src, &flaky);
    match result {
        Err(_) => {}
        Ok(buckets) => {
            let ids: Vec<_> = buckets.iter().map(|b| b.bucket_id.clone()).collect();
            assert!(
                ids.iter().any(|id| id.starts_with("win")),
                "sync_datastores returned Ok({ids:?}): the failed 'win' bucket is not reported \
                 anywhere, so the peer is recorded as Imported with no warning"
            );
        }
    }
}

/// A single pulled bucket with hostname "unknown" aborts the whole peer:
/// the `.map(..)?` / collect short-circuits before the per-bucket skip loop,
/// so every healthy sibling bucket of that peer is never synced.
#[test]
fn unknown_hostname_bucket_blocks_all_sibling_buckets_on_pull() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    src.create_bucket(&bucket("win", "phone")).unwrap();
    src.create_bucket(&bucket("legacy", "unknown")).unwrap();
    fill(&src, "win", 10);

    let _ = pull(&src, &dest);
    let dest_buckets = dest.get_buckets().unwrap();
    assert!(
        dest_buckets.contains_key("win-synced-from-phone"),
        "healthy bucket was not synced because a sibling had hostname 'unknown'; dest = {:?}",
        dest_buckets.keys().collect::<Vec<_>>()
    );
}
