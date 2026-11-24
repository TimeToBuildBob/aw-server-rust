//! Edge-case input bug hunt for aw-sync (bughunt 2026-11-24).
//! Each test here is expected to FAIL on master ea6f09c.

use aw_datastore::Datastore;
use aw_models::{Bucket, BucketMetadata, Event};
use aw_sync::{sync_datastores, AccessMethod, SyncSpec};
use chrono::{DateTime, Duration, TimeZone, Utc};

fn bucket(id: &str, hostname: &str) -> Bucket {
    Bucket {
        bid: None,
        id: id.to_string(),
        _type: "currentwindow".to_string(),
        client: "aw-watcher-window".to_string(),
        hostname: hostname.to_string(),
        created: None,
        data: serde_json::Map::new(),
        metadata: BucketMetadata::default(),
        events: None,
        last_updated: None,
    }
}

fn ev(ts: DateTime<Utc>, secs: i64, title: &str) -> Event {
    let mut data = serde_json::Map::new();
    data.insert("title".into(), serde_json::json!(title));
    Event {
        id: None,
        timestamp: ts,
        duration: Duration::seconds(secs),
        data,
    }
}

fn base() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 1, 12, 0, 0).unwrap()
}

fn ids(ds: &dyn AccessMethod) -> Vec<String> {
    let mut v: Vec<String> = ds.get_buckets().unwrap().keys().cloned().collect();
    v.sort();
    v
}

/// One peer bucket whose hostname is the literal "unknown" sentinel aborts the
/// pull of *every* bucket from that peer: the refusal is raised inside the
/// `collect::<Result<_>>()?` over the bucket list, not in the per-bucket loop.
/// The sibling `" * "` case (which sanitizes to unknown) is skipped per-bucket,
/// so the healthy sibling syncs there — but not here.
#[test]
fn unknown_hostname_bucket_does_not_block_healthy_sibling_on_pull() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    src.create_bucket(&bucket("aw-watcher-afk_legacy", "unknown"))
        .unwrap();
    src.create_bucket(&bucket("aw-watcher-window_hostb", "hostb"))
        .unwrap();
    src.insert_events("aw-watcher-window_hostb", &[ev(base(), 10, "a")])
        .unwrap();
    src.force_commit().unwrap();

    let res = sync_datastores(&src, &dest, false, None, &SyncSpec::default());
    let dest_ids = ids(&dest);
    assert!(
        dest_ids.contains(&"aw-watcher-window_hostb-synced-from-hostb".to_string()),
        "healthy sibling must still be pulled; result={res:?}, dest buckets={dest_ids:?}"
    );
}

/// A host whose hostname has no ASCII alphanumerics (e.g. a Windows machine
/// named in Cyrillic) is pushed into the sync folder with its hostname
/// rewritten to the "unknown" sentinel (push sanitizes *after* the
/// `hostname == "unknown"` device-id substitution). Every peer then refuses
/// to pull it, so that host's data never reaches any other device.
#[test]
fn non_ascii_hostname_survives_push_then_pull() {
    let local = Datastore::new_in_memory(false);
    let staging = Datastore::new_in_memory(false);
    let peer = Datastore::new_in_memory(false);
    let bid = "aw-watcher-window_компьютер";
    local.create_bucket(&bucket(bid, "компьютер")).unwrap();
    local.insert_events(bid, &[ev(base(), 10, "a")]).unwrap();
    local.force_commit().unwrap();

    sync_datastores(
        &local,
        &staging,
        true,
        Some("3f2b9c1e-device-id"),
        &SyncSpec::default(),
    )
    .expect("push should succeed");
    let staged = staging.get_bucket(bid).unwrap();
    let pull = sync_datastores(&staging, &peer, false, None, &SyncSpec::default());
    let peer_ids = ids(&peer);
    assert!(
        pull.is_ok() && peer_ids.len() == 1,
        "pushed staging hostname={:?}; pull result={pull:?}; peer buckets={peer_ids:?}",
        staged.hostname
    );
}

/// Two different devices whose (non-ASCII) names sanitize to the same string
/// are merged into one `-synced-from-` destination bucket when the bucket id
/// itself does not contain the hostname (Android: `aw-watcher-android-test`).
#[test]
fn distinct_devices_with_colliding_sanitized_names_are_not_merged() {
    let phone_a = Datastore::new_in_memory(false);
    let phone_b = Datastore::new_in_memory(false);
    let desktop = Datastore::new_in_memory(false);
    let bid = "aw-watcher-android-test";
    phone_a.create_bucket(&bucket(bid, "Galaxy Маши")).unwrap();
    phone_b.create_bucket(&bucket(bid, "Galaxy Пети")).unwrap();
    phone_a
        .insert_events(bid, &[ev(base(), 10, "masha-app")])
        .unwrap();
    phone_b
        .insert_events(bid, &[ev(base() + Duration::hours(1), 10, "petya-app")])
        .unwrap();
    phone_a.force_commit().unwrap();
    phone_b.force_commit().unwrap();

    sync_datastores(&phone_a, &desktop, false, None, &SyncSpec::default()).unwrap();
    sync_datastores(&phone_b, &desktop, false, None, &SyncSpec::default()).unwrap();
    let dest_ids = ids(&desktop);
    let mut per_bucket = Vec::new();
    for id in &dest_ids {
        let titles: Vec<String> = desktop
            .get_events(id, None, None, None)
            .unwrap()
            .iter()
            .map(|e| e.data["title"].as_str().unwrap().to_string())
            .collect();
        per_bucket.push((id.clone(), titles));
    }
    assert_eq!(
        dest_ids.len(),
        2,
        "two devices must not share one destination bucket: {per_bucket:?}"
    );
}

/// A tie run of more than BATCH_SIZE (5000) events at one timestamp: the
/// "pathological" branch accepts the full page and continues at ts-1ns, so
/// every event of the run past the first 5000 is never synced.
#[test]
fn tie_run_larger_than_a_page_is_synced_in_full() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    let bid = "bucket-tie";
    src.create_bucket(&bucket(bid, "hostb")).unwrap();
    let events: Vec<Event> = (0..5003).map(|i| ev(base(), 0, &format!("e{i}"))).collect();
    src.insert_events(bid, &events).unwrap();
    src.force_commit().unwrap();

    sync_datastores(&src, &dest, false, None, &SyncSpec::default()).unwrap();
    let n = AccessMethod::get_event_count(&dest, "bucket-tie-synced-from-hostb").unwrap();
    assert_eq!(n, 5003, "first pass must copy the whole tie run");
}

/// Second pass over the same >5000 tie run re-inserts a full page without the
/// boundary dedup (which only runs on the last page): duplicates every pass.
#[test]
fn tie_run_larger_than_a_page_is_not_duplicated_on_repass() {
    let src = Datastore::new_in_memory(false);
    let dest = Datastore::new_in_memory(false);
    let bid = "bucket-tie2";
    src.create_bucket(&bucket(bid, "hostb")).unwrap();
    let events: Vec<Event> = (0..5003).map(|i| ev(base(), 0, &format!("e{i}"))).collect();
    src.insert_events(bid, &events).unwrap();
    src.force_commit().unwrap();

    sync_datastores(&src, &dest, false, None, &SyncSpec::default()).unwrap();
    let n1 = AccessMethod::get_event_count(&dest, "bucket-tie2-synced-from-hostb").unwrap();
    sync_datastores(&src, &dest, false, None, &SyncSpec::default()).unwrap();
    let n2 = AccessMethod::get_event_count(&dest, "bucket-tie2-synced-from-hostb").unwrap();
    assert_eq!(n1, n2, "a no-op re-pass must not add rows ({n1} -> {n2})");
}

// ---------------------------------------------------------------------------
// CLI / dedupe tests: drive the real binaries.
// ---------------------------------------------------------------------------

fn scratch(name: &str) -> std::path::PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::path::PathBuf::from(format!(
        "/tmp/bob-scratch/d0e0/edge/{name}-{}-{ts}",
        std::process::id()
    ));
    for sub in ["home", "cfg", "data", "cache", "sync"] {
        std::fs::create_dir_all(p.join(sub)).unwrap();
    }
    p
}

fn aw_sync_cmd(dir: &std::path::Path, port: u16) -> std::process::Command {
    let mut c = std::process::Command::new(env!("CARGO_BIN_EXE_aw-sync"));
    c.env("HOME", dir.join("home"))
        .env("XDG_CONFIG_HOME", dir.join("cfg"))
        .env("XDG_DATA_HOME", dir.join("data"))
        .env("XDG_CACHE_HOME", dir.join("cache"))
        .env("AW_SYNC_DIR", dir.join("sync"))
        .args(["--testing", "--port", &port.to_string()]);
    c
}

/// Every list-valued flag (`sync --buckets`, `sync --host`, `daemon --buckets`,
/// `dedupe --bucket`) is declared `Option<Vec<String>>` with a
/// `value_parser` that itself returns `Vec<String>`; clap's derive then
/// panics on access ("Mismatch between definition and access"). Port 9 is
/// closed, so a correctly-parsed invocation exits with a connection error,
/// never with a panic (exit 101).
#[test]
fn list_valued_cli_flags_do_not_panic() {
    let dir = scratch("cli");
    let mut failures = Vec::new();
    for args in [
        vec!["sync", "--buckets", "aw-watcher-window_a"],
        vec!["sync", "--host", "hostb"],
        vec!["dedupe", "--dry-run", "--bucket", "x-synced-from-hostb"],
    ] {
        let out = aw_sync_cmd(&dir, 9).args(&args).output().unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        if out.status.code() == Some(101) || stderr.contains("Mismatch between definition") {
            let line = stderr
                .lines()
                .find(|l| l.contains("Mismatch"))
                .unwrap_or("")
                .to_string();
            failures.push(format!("{args:?} -> {:?}: {line}", out.status.code()));
        }
    }
    assert!(
        failures.is_empty(),
        "CLI panicked:\n{}",
        failures.join("\n")
    );
}

/// `aw-sync dedupe` pages with `end = cut - 1ns`; the server clips the
/// duration of every event that ends after that bound. Two *distinct* events
/// with the same start and data but different durations that both reach past
/// the cut therefore come back with identical (clipped) durations and are
/// reported (and, without --dry-run, deleted) as exact duplicates.
#[test]
fn dedupe_does_not_flag_distinct_durations_straddling_page_cut() {
    let aw_server = std::path::Path::new(env!("CARGO_BIN_EXE_aw-sync")).with_file_name("aw-server");
    assert!(
        aw_server.exists(),
        "build aw-server first: {}",
        aw_server.display()
    );
    let dir = scratch("dedupe");
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let mut server = std::process::Command::new(&aw_server)
        .env("HOME", dir.join("home"))
        .env("XDG_CONFIG_HOME", dir.join("cfg"))
        .env("XDG_DATA_HOME", dir.join("data"))
        .env("XDG_CACHE_HOME", dir.join("cache"))
        .args([
            "--testing",
            "--no-legacy-import",
            "--port",
            &port.to_string(),
        ])
        .arg("--dbpath")
        .arg(dir.join("srv.db"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    struct Kill(std::process::Child);
    impl Drop for Kill {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _guard = Kill(std::mem::replace(
        &mut server,
        std::process::Command::new("true").spawn().unwrap(),
    ));

    let client = aw_client_rust::blocking::AwClient::new("127.0.0.1", port, "bughunt").unwrap();
    client.wait_for_start().unwrap();
    let bid = "aw-watcher-window_x-synced-from-hostb";
    client.create_bucket_simple(bid, "currentwindow").unwrap();
    // 5000 (= dedupe PAGE_SIZE) newer 1s events, then two distinct events at
    // T0 ("a", 150s and "a", 200s) that both overlap the first page's cut at
    // T0+100s, then one old event.
    let t0 = base();
    let mut events: Vec<Event> = (0..5000)
        .map(|i| ev(t0 + Duration::seconds(100 + i), 1, &format!("w{i}")))
        .collect();
    events.push(ev(t0, 150, "a"));
    events.push(ev(t0, 200, "a"));
    events.push(ev(t0 - Duration::seconds(1000), 5, "z"));
    client.insert_events(bid, events).unwrap();

    let out = aw_sync_cmd(&dir, port)
        .args(["dedupe", "--dry-run"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("No duplicates found"),
        "no two events are identical, but dedupe reported: {stdout}"
    );
}
