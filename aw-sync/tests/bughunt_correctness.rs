//! Bug-hunt (correctness lens): sync_one / reconcile_updated_events defects.
//! Every test here is expected to FAIL on master ea6f09c.
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::json;

use aw_datastore::Datastore;
use aw_models::{Bucket, Event};
use aw_sync::SyncSpec;

fn create_bucket(ds: &Datastore, id: &str, btype: &str, hostname: &str) -> String {
    let bucket: Bucket = serde_json::from_value(json!({
        "id": id, "type": btype, "hostname": hostname, "client": "test"
    }))
    .unwrap();
    ds.create_bucket(&bucket).unwrap();
    ds.force_commit().unwrap();
    id.to_string()
}

fn ev(ts: DateTime<Utc>, dur: Duration, data: serde_json::Value) -> Event {
    Event {
        id: None,
        timestamp: ts,
        duration: dur,
        data: data.as_object().unwrap().clone(),
    }
}

fn pull(src: &Datastore, dest: &Datastore) -> Result<Vec<aw_sync::BucketReport>, String> {
    aw_sync::sync_datastores(src, dest, false, None, &SyncSpec::default())
}

fn all_sorted(ds: &Datastore, bucket: &str) -> Vec<Event> {
    let mut v = ds.get_events(bucket, None, None, None).unwrap();
    v.sort_by_key(|e| (e.timestamp, e.duration));
    v
}

/// Replace an event in place (same timestamp + duration, new data) the way the
/// WebUI / Android edit path does: delete + insert.
fn edit_data(ds: &Datastore, bucket: &str, ts: DateTime<Utc>, data: serde_json::Value) {
    let target = ds
        .get_events(bucket, None, None, None)
        .unwrap()
        .into_iter()
        .find(|e| e.timestamp == ts)
        .unwrap();
    ds.delete_events_by_id(bucket, vec![target.id.unwrap()])
        .unwrap();
    ds.insert_events(bucket, &[ev(target.timestamp, target.duration, data)])
        .unwrap();
    ds.force_commit().unwrap();
}

/// F1: an edited event that straddles `resume - 7d` is replaced on the
/// destination by a copy clipped to start at the lookback boundary; the part of
/// the event before the boundary is deleted permanently.
#[test]
fn reconcile_edit_of_event_straddling_lookback_start_truncates_it() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    let b = create_bucket(&src, "aw-watcher-afk_laptop", "afkstatus", "laptop");
    let base = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
    // Long event (10 days) followed by a short one. resume = base+10d+1h,
    // lookback_start = base+3d+1h, which falls inside the long event.
    src.insert_events(
        &b,
        &[
            ev(
                base,
                Duration::days(10),
                json!({"status": "afk", "note": "a"}),
            ),
            ev(
                base + Duration::days(10),
                Duration::hours(1),
                json!({"status": "not-afk"}),
            ),
        ],
    )
    .unwrap();
    src.force_commit().unwrap();
    pull(&src, &dest).unwrap();
    let dest_b = "aw-watcher-afk_laptop-synced-from-laptop";
    assert_eq!(all_sorted(&dest, dest_b).len(), 2);

    // Owner edits the long event's data (same timestamp + duration).
    edit_data(&src, &b, base, json!({"status": "afk", "note": "edited"}));
    pull(&src, &dest).unwrap();

    let got = all_sorted(&dest, dest_b);
    let long = got
        .iter()
        .find(|e| e.data.get("status") == Some(&json!("afk")))
        .unwrap();
    assert_eq!(
        (
            long.timestamp,
            long.duration,
            long.data.get("note").cloned()
        ),
        (base, Duration::days(10), Some(json!("edited"))),
        "edited straddling event was truncated on dest: {got:?}"
    );
}

/// F2: a zero-duration latest event (e.g. a running stopwatch timer) that is
/// later replaced on the source (stopped: data + duration change) is never
/// replaced on the destination: the stale running row stays next to the new one.
#[test]
fn stopped_stopwatch_leaves_phantom_running_row_on_dest() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    let b = create_bucket(&src, "aw-stopwatch", "general.stopwatch", "laptop");
    let t = Utc::now() - Duration::hours(1);
    let inserted = src
        .insert_events(
            &b,
            &[ev(
                t,
                Duration::zero(),
                json!({"label": "work", "running": true}),
            )],
        )
        .unwrap();
    src.force_commit().unwrap();
    pull(&src, &dest).unwrap();

    // Stop the timer: aw-webui replaces the event by id with the final
    // duration and running=false.
    let mut stopped = ev(
        t,
        Duration::minutes(25),
        json!({"label": "work", "running": false}),
    );
    stopped.id = inserted[0].id;
    src.insert_events(&b, &[stopped]).unwrap();
    src.force_commit().unwrap();
    assert_eq!(src.get_events(&b, None, None, None).unwrap().len(), 1);

    pull(&src, &dest).unwrap();
    let got = all_sorted(&dest, "aw-stopwatch-synced-from-laptop");
    assert_eq!(
        got.len(),
        1,
        "dest should mirror the single stopped timer, got {got:?}"
    );
}

/// F2b: same root cause without the stopwatch — a title edit of a
/// zero-duration latest event duplicates it instead of replacing it.
#[test]
fn title_edit_of_zero_duration_latest_event_duplicates_it() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    let b = create_bucket(&src, "aw-watcher-window_laptop", "currentwindow", "laptop");
    let t0 = Utc::now() - Duration::hours(2);
    let t1 = t0 + Duration::minutes(10);
    src.insert_events(
        &b,
        &[
            ev(t0, Duration::minutes(10), json!({"app": "a", "title": "x"})),
            ev(t1, Duration::zero(), json!({"app": "b", "title": "old"})),
        ],
    )
    .unwrap();
    src.force_commit().unwrap();
    pull(&src, &dest).unwrap();
    edit_data(&src, &b, t1, json!({"app": "b", "title": "new"}));
    pull(&src, &dest).unwrap();
    let got = all_sorted(&dest, "aw-watcher-window_laptop-synced-from-laptop");
    let titles: Vec<_> = got
        .iter()
        .map(|e| e.data.get("title").cloned().unwrap())
        .collect();
    assert_eq!(
        titles,
        vec![json!("x"), json!("new")],
        "stale title must be replaced, got {got:?}"
    );
}

/// F3: reconcile deleting a pre-existing destination duplicate makes the event
/// count shrink, which sync_one then reports as a failure of the whole bucket
/// (and, for a single-bucket peer, of the whole pull) even though nothing went
/// wrong.
#[test]
fn reconcile_dedup_reports_bucket_failure() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    let b = create_bucket(&src, "aw-watcher-window_laptop", "currentwindow", "laptop");
    let t0 = Utc::now() - Duration::hours(3);
    let t1 = t0 + Duration::minutes(10);
    src.insert_events(
        &b,
        &[
            ev(t0, Duration::minutes(10), json!({"title": "x"})),
            ev(t1, Duration::minutes(5), json!({"title": "y"})),
        ],
    )
    .unwrap();
    src.force_commit().unwrap();
    pull(&src, &dest).unwrap();
    // Simulate a pre-#713 duplicate row already in the destination.
    let dest_b = "aw-watcher-window_laptop-synced-from-laptop";
    dest.insert_events(
        dest_b,
        &[ev(t0, Duration::minutes(10), json!({"title": "x"}))],
    )
    .unwrap();
    dest.force_commit().unwrap();
    let r = pull(&src, &dest);
    assert!(r.is_ok(), "no-op pass reported failure: {r:?}");
}

/// F2c: two overlapping stopwatch timers. Timer 1 is stopped after timer 2
/// started and both running rows were synced. The stopped timer 1 arrives on
/// dest clipped to start at timer 2's start (25 min recorded as 15 min), next
/// to the phantom running row.
#[test]
fn stopped_overlapping_timer_is_truncated_on_dest() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    let b = create_bucket(&src, "aw-stopwatch", "general.stopwatch", "laptop");
    let t1 = Utc::now() - Duration::hours(1);
    let t2 = t1 + Duration::minutes(10);
    let s1 = src
        .insert_events(
            &b,
            &[ev(
                t1,
                Duration::zero(),
                json!({"label": "work", "running": true}),
            )],
        )
        .unwrap();
    src.insert_events(
        &b,
        &[ev(
            t2,
            Duration::zero(),
            json!({"label": "call", "running": true}),
        )],
    )
    .unwrap();
    src.force_commit().unwrap();
    pull(&src, &dest).unwrap();

    let mut stopped = ev(
        t1,
        Duration::minutes(25),
        json!({"label": "work", "running": false}),
    );
    stopped.id = s1[0].id;
    src.insert_events(&b, &[stopped]).unwrap();
    src.force_commit().unwrap();
    pull(&src, &dest).unwrap();

    let got = all_sorted(&dest, "aw-stopwatch-synced-from-laptop");
    let work_stopped: Vec<_> = got
        .iter()
        .filter(|e| e.data.get("running") == Some(&json!(false)))
        .map(|e| (e.timestamp, e.duration))
        .collect();
    assert_eq!(
        work_stopped,
        vec![(t1, Duration::minutes(25))],
        "stopped timer must keep its start and duration; dest = {got:?}"
    );
}
