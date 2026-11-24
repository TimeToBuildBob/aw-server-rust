//! Bug-hunt reproductions (concurrency / ordering lens). Each test asserts the
//! correct behaviour and is expected to FAIL on master ea6f09c.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use chrono::{DateTime, Duration, TimeZone, Utc};

use aw_datastore::{Datastore, DatastoreError};
use aw_models::{Bucket, Event};
use aw_sync::{sync_datastores, AccessMethod, SyncSpec};

const BUCKET: &str = "aw-watcher-window_laptop";
// Production BATCH_SIZE for integration tests (the 5 override is unit-test only).
const PROD_BATCH: usize = 5000;

fn spec() -> SyncSpec {
    SyncSpec {
        path: std::path::PathBuf::from("/tmp/bob-scratch/d0e0/concurrency"),
        path_db: None,
        buckets: None,
        start: None,
    }
}

fn mk_bucket(ds: &Datastore) {
    let b: Bucket = serde_json::from_value(serde_json::json!({
        "id": BUCKET,
        "type": "currentwindow",
        "hostname": "laptop",
        "client": "test",
    }))
    .unwrap();
    ds.create_bucket(&b).unwrap();
}

fn ev(ts: DateTime<Utc>, i: usize) -> Event {
    serde_json::from_value(serde_json::json!({
        "timestamp": ts.to_rfc3339(),
        "duration": 0.5,
        "data": {"app": "a", "title": format!("t{i}")},
    }))
    .unwrap()
}

fn base() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap()
}

/// `n` events, 1 s apart, unique data.
fn fill(ds: &Datastore, start_idx: usize, n: usize, t0: DateTime<Utc>) {
    let events: Vec<Event> = (0..n)
        .map(|i| ev(t0 + Duration::seconds(i as i64), start_idx + i))
        .collect();
    ds.insert_events(BUCKET, &events).unwrap();
    ds.force_commit().unwrap();
}

fn count(ds: &Datastore, bucket: &str) -> i64 {
    ds.get_event_count(bucket, None, None).unwrap()
}

fn synced_id(ds: &Datastore) -> String {
    ds.get_buckets()
        .unwrap()
        .into_keys()
        .find(|k| k.starts_with(BUCKET))
        .expect("destination bucket")
}

/// Source wrapper: delegates to `inner`, but runs `hook` once, the first time a
/// second (older) page is requested (i.e. `end` is set).
struct PageHook<'a> {
    inner: &'a Datastore,
    hook: RefCell<Option<Box<dyn FnOnce() -> Result<(), String> + 'a>>>,
}

impl std::fmt::Debug for PageHook<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PageHook")
    }
}

impl AccessMethod for PageHook<'_> {
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
        if end.is_some() && limit == Some(PROD_BATCH as u64) {
            if let Some(h) = self.hook.borrow_mut().take() {
                h()?;
            }
        }
        AccessMethod::get_events(self.inner, bucket_id, start, end, limit)
    }
    fn insert_events(&self, bucket_id: &str, events: Vec<Event>) -> Result<(), String> {
        AccessMethod::insert_events(self.inner, bucket_id, events)
    }
    fn get_event_count(&self, bucket_id: &str) -> Result<i64, String> {
        AccessMethod::get_event_count(self.inner, bucket_id)
    }
    fn heartbeat(&self, bucket_id: &str, event: Event, duration: f64) -> Result<(), String> {
        AccessMethod::heartbeat(self.inner, bucket_id, event, duration)
    }
    fn delete_events_by_id(&self, bucket_id: &str, ids: Vec<i64>) -> Result<(), String> {
        AccessMethod::delete_events_by_id(self.inner, bucket_id, ids)
    }
}

/// Finding 1: a multi-page sync writes the NEWEST page first. If the pass dies
/// before the older pages land (daemon SIGTERM at logout, aw-server restart,
/// HTTP timeout on a later page), the destination's newest event is already
/// the source's newest, so every later pass resumes past the hole and the
/// older events are never synced.
#[test]
fn interrupted_multipage_push_leaves_permanent_gap() {
    let src = Datastore::new_in_memory(false);
    let staging = Datastore::new_in_memory(false);
    mk_bucket(&src);
    let total = PROD_BATCH + 3;
    fill(&src, 0, total, base());

    // Pass 1: fails when fetching the second (older) page.
    let failing = PageHook {
        inner: &src,
        hook: RefCell::new(Some(Box::new(|| Err("simulated interruption".to_string())))),
    };
    let r1 = sync_datastores(&failing, &staging, true, Some("dev-laptop"), &spec());
    println!("pass 1 (interrupted): {r1:?}");
    let after1 = count(&staging, BUCKET);

    // Pass 2..3: normal retries, nothing fails.
    let r2 = sync_datastores(&src, &staging, true, Some("dev-laptop"), &spec()).unwrap();
    let r3 = sync_datastores(&src, &staging, true, Some("dev-laptop"), &spec()).unwrap();
    println!(
        "after pass1={after1}, pass2 events_new={}, pass3 events_new={}",
        r2[0].events_new, r3[0].events_new
    );
    assert_eq!(
        count(&staging, BUCKET),
        total as i64,
        "retries after an interrupted multi-page pass must converge to the full source"
    );
}

/// Finding 1, cross-device form, with NO failure anywhere: a peer that pulls
/// the owner's staging db while the owner's multi-page push is between pages
/// (Syncthing delivering a committed mid-push snapshot) sees only the newest
/// page. Its cursor jumps to the newest event, so once the push completes the
/// older events are behind the peer's cursor forever.
#[test]
fn peer_pull_of_mid_push_snapshot_permanently_misses_older_events() {
    let src = Datastore::new_in_memory(false);
    let staging = Datastore::new_in_memory(false);
    let peer = Datastore::new_in_memory(false);
    mk_bucket(&src);
    let total = PROD_BATCH + 3;
    fill(&src, 0, total, base());

    let staging_ref = &staging;
    let peer_ref = &peer;
    let pull_mid_push = PageHook {
        inner: &src,
        hook: RefCell::new(Some(Box::new(move || {
            sync_datastores(staging_ref, peer_ref, false, None, &spec()).map(|_| ())
        }))),
    };
    sync_datastores(&pull_mid_push, &staging, true, Some("dev-laptop"), &spec()).unwrap();
    assert_eq!(
        count(&staging, BUCKET),
        total as i64,
        "push itself completed"
    );

    // Later pulls see the complete staging db.
    sync_datastores(&staging, &peer, false, None, &spec()).unwrap();
    sync_datastores(&staging, &peer, false, None, &spec()).unwrap();
    let id = synced_id(&peer);
    assert_eq!(
        count(&peer, &id),
        total as i64,
        "peer must eventually hold every event of the completed staging db"
    );
}

/// Destination wrapper that runs a whole second sync pass (another aw-sync
/// process: aw-qt's daemon plus a manual `aw-sync sync`, or two Android sync
/// triggers) right after this pass has read its resume cursor and before its
/// first write. There is no single-instance lock in aw-sync.
struct Interleave<'a> {
    inner: &'a Datastore,
    src: &'a Datastore,
    fired: Cell<bool>,
}

impl std::fmt::Debug for Interleave<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Interleave")
    }
}

impl Interleave<'_> {
    fn other_pass(&self) {
        if !self.fired.replace(true) {
            sync_datastores(self.src, self.inner, false, None, &spec()).unwrap();
        }
    }
}

impl AccessMethod for Interleave<'_> {
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
        self.other_pass();
        AccessMethod::insert_events(self.inner, bucket_id, events)
    }
    fn get_event_count(&self, bucket_id: &str) -> Result<i64, String> {
        AccessMethod::get_event_count(self.inner, bucket_id)
    }
    fn heartbeat(&self, bucket_id: &str, event: Event, duration: f64) -> Result<(), String> {
        self.other_pass();
        AccessMethod::heartbeat(self.inner, bucket_id, event, duration)
    }
    fn delete_events_by_id(&self, bucket_id: &str, ids: Vec<i64>) -> Result<(), String> {
        AccessMethod::delete_events_by_id(self.inner, bucket_id, ids)
    }
}

/// Finding 2: two overlapping pull passes into the same destination both
/// compute the resume cursor from the destination's (same) newest event and
/// both insert the new events: permanent duplicates in the synced bucket.
#[test]
fn overlapping_pull_passes_duplicate_events() {
    let peer_staging = Datastore::new_in_memory(false);
    let local = Datastore::new_in_memory(false);
    mk_bucket(&peer_staging);
    fill(&peer_staging, 0, 3, base());
    // Earlier, non-overlapping pass: bucket exists, 3 events imported.
    sync_datastores(&peer_staging, &local, false, None, &spec()).unwrap();
    // Peer recorded 4 more events since.
    fill(&peer_staging, 3, 4, base() + Duration::minutes(10));

    let dest = Interleave {
        inner: &local,
        src: &peer_staging,
        fired: Cell::new(false),
    };
    sync_datastores(&peer_staging, &dest, false, None, &spec()).unwrap();
    let id = synced_id(&local);
    assert_eq!(
        count(&local, &id),
        count(&peer_staging, BUCKET),
        "overlapping passes must not import the same events twice"
    );
}

/// Finding 3: clock skew. One event written while the device clock was a year
/// ahead (dead RTC / bad NTP at boot) moves the resume cursor a year into the
/// future; after the clock is corrected, every new event is "older than dest
/// newest" and the bucket silently stops syncing until the wall clock passes
/// the bad timestamp. The pass reports "up to date".
#[test]
fn future_dated_event_from_clock_skew_stalls_bucket() {
    let src = Datastore::new_in_memory(false);
    let staging = Datastore::new_in_memory(false);
    mk_bucket(&src);
    fill(&src, 0, 3, base());
    // One event stamped while the clock was one year ahead.
    src.insert_events(BUCKET, &[ev(base() + Duration::days(365), 99)])
        .unwrap();
    src.force_commit().unwrap();
    sync_datastores(&src, &staging, true, Some("dev-laptop"), &spec()).unwrap();
    assert_eq!(count(&staging, BUCKET), 4);

    // Clock corrected; a day of normal activity follows.
    fill(&src, 100, 50, base() + Duration::hours(1));
    let r = sync_datastores(&src, &staging, true, Some("dev-laptop"), &spec()).unwrap();
    println!("second pass events_new={}", r[0].events_new);
    assert_eq!(
        count(&staging, BUCKET),
        count(&src, BUCKET),
        "events recorded after the clock was corrected must still sync"
    );
}
