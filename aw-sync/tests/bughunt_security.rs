//! Security / path-handling bughunt repros (2026-11-24).
//!
//! Every test here asserts the *safe* behaviour and is expected to FAIL on
//! master ea6f09c, demonstrating the defect. Temp data lives only under
//! /tmp/bob-scratch/d0e0/security/.
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use aw_client_rust::blocking::AwClient;
use aw_datastore::Datastore;
use aw_models::{Bucket, BucketMetadata, Event};
use aw_sync::{create_datastore, sync_datastores, AccessMethod, SyncMode, SyncSpec};
use chrono::{DateTime, Duration, TimeZone, Utc};

const SCRATCH: &str = "/tmp/bob-scratch/d0e0/security";
static COUNTER: AtomicU64 = AtomicU64::new(0);
/// pull_all reads AW_SYNC_DIR from the process env; serialize server tests.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn scratch(label: &str) -> PathBuf {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = PathBuf::from(SCRATCH).join(format!(
        "{label}-{}-{}-{ts}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

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

fn event(ts: DateTime<Utc>, secs: i64, app: &str) -> Event {
    Event {
        id: None,
        timestamp: ts,
        duration: Duration::seconds(secs),
        data: serde_json::from_value(serde_json::json!({ "app": app })).unwrap(),
    }
}

fn apps(ds: &dyn AccessMethod, bucket_id: &str) -> Vec<String> {
    let mut v: Vec<String> = ds
        .get_events(bucket_id, None, None, None)
        .unwrap()
        .into_iter()
        .map(|e| e.data["app"].as_str().unwrap_or("?").to_string())
        .collect();
    v.sort();
    v
}

// ---------------------------------------------------------------------------
// Finding: pull derives the destination bucket from peer-controlled metadata
// (`$aw.sync.origin` / bucket hostname), not from the folder it was read from.
// Any device in the shared folder can write into another peer's imported
// history. A single far-future event then freezes that peer's resume cursor.
// ---------------------------------------------------------------------------
#[test]
fn peer_spoofing_origin_freezes_another_peers_import() {
    let spec = SyncSpec::default();
    let local = Datastore::new_in_memory(false);

    // Honest peer "alice" (its own folder/db).
    let alice = Datastore::new_in_memory(false);
    alice
        .create_bucket(&bucket("aw-watcher-window_alice", "alice"))
        .unwrap();
    let t0 = Utc::now() - Duration::hours(2);
    alice
        .insert_events("aw-watcher-window_alice", &[event(t0, 60, "alice-1")])
        .unwrap();
    sync_datastores(&alice, &local, false, None, &spec).unwrap();
    let dest = "aw-watcher-window_alice-synced-from-alice";
    assert_eq!(apps(&local, dest), vec!["alice-1"], "precondition");

    // Malicious peer "mallory", in its OWN folder, claims origin "alice".
    let mallory = Datastore::new_in_memory(false);
    let mut b = bucket("aw-watcher-window_alice", "mallory");
    b.data
        .insert("$aw.sync.origin".into(), serde_json::json!("alice"));
    mallory.create_bucket(&b).unwrap();
    let far_future = Utc.with_ymd_and_hms(2099, 1, 1, 0, 0, 0).unwrap();
    mallory
        .insert_events(
            "aw-watcher-window_alice",
            &[event(far_future, 1, "forged-by-mallory")],
        )
        .unwrap();
    sync_datastores(&mallory, &local, false, None, &spec).unwrap();

    // Alice keeps working; her next pass is pulled normally.
    alice
        .insert_events(
            "aw-watcher-window_alice",
            &[event(Utc::now() - Duration::minutes(10), 60, "alice-2")],
        )
        .unwrap();
    sync_datastores(&alice, &local, false, None, &spec).unwrap();

    let got = apps(&local, dest);
    eprintln!("local '{dest}' after alice/mallory/alice pulls: {got:?}");
    assert!(
        !got.contains(&"forged-by-mallory".to_string()),
        "a peer in its own folder wrote into alice's imported bucket: {got:?}"
    );
    assert!(
        got.contains(&"alice-2".to_string()),
        "alice's new event was never imported (resume cursor frozen at 2099): {got:?}"
    );
}

// Same root cause: the edit-reconcile path treats the spoofing peer as the
// "owner" and DELETES alice's already-imported event, replacing it.
#[test]
fn peer_spoofing_origin_deletes_another_peers_imported_event() {
    let spec = SyncSpec::default();
    let local = Datastore::new_in_memory(false);

    let alice = Datastore::new_in_memory(false);
    alice
        .create_bucket(&bucket("aw-watcher-window_alice", "alice"))
        .unwrap();
    // Timestamp + duration are visible to every participant (alice's sync.db
    // is replicated to all of them).
    let t0 = Utc::now() - Duration::hours(3);
    let t1 = t0 + Duration::minutes(30);
    alice
        .insert_events(
            "aw-watcher-window_alice",
            &[event(t0, 60, "alice-real"), event(t1, 60, "alice-latest")],
        )
        .unwrap();
    sync_datastores(&alice, &local, false, None, &spec).unwrap();
    let dest = "aw-watcher-window_alice-synced-from-alice";
    assert_eq!(apps(&local, dest), vec!["alice-latest", "alice-real"]);

    let mallory = Datastore::new_in_memory(false);
    mallory
        .create_bucket(&bucket("aw-watcher-window_alice", "Alice")) // hostname only, no metadata
        .unwrap();
    mallory
        .insert_events(
            "aw-watcher-window_alice",
            &[event(t0, 60, "rewritten-by-mallory")],
        )
        .unwrap();
    sync_datastores(&mallory, &local, false, None, &spec).unwrap();

    let got = apps(&local, dest);
    eprintln!("local '{dest}' after mallory pull: {got:?}");
    assert!(
        got.contains(&"alice-real".to_string()),
        "mallory's pull deleted alice's imported event: {got:?}"
    );
}

// ---------------------------------------------------------------------------
// Server-backed helpers (spawn the built aw-server binary on a temp db).
// ---------------------------------------------------------------------------
struct Server {
    child: Child,
    client: AwClient,
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn server_bin() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/debug/aw-server")
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn isolate_env(root: &Path) {
    // Never touch ~/.local/share etc. from this test process.
    std::env::set_var("XDG_DATA_HOME", root.join("xdg-data"));
    std::env::set_var("XDG_CONFIG_HOME", root.join("xdg-config"));
    std::env::set_var("XDG_CACHE_HOME", root.join("xdg-cache"));
    std::env::set_var("AW_SYNC_LAST_REPORT", root.join("last-report.json"));
}

fn start_server(root: &Path, device_id: &str) -> Server {
    let bin = server_bin();
    assert!(bin.exists(), "build aw-server first: {}", bin.display());
    let port = free_port();
    let child = Command::new(bin)
        .args(["--testing", "--no-legacy-import", "--port"])
        .arg(port.to_string())
        .arg("--dbpath")
        .arg(root.join("server.db"))
        .args(["--device-id", device_id])
        .env("XDG_DATA_HOME", root.join("srv-data"))
        .env("XDG_CONFIG_HOME", root.join("srv-config"))
        .env("XDG_CACHE_HOME", root.join("srv-cache"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let client = AwClient::new("127.0.0.1", port, "aw-sync-bughunt").unwrap();
    for _ in 0..200 {
        if client.get_info().is_ok() {
            return Server { child, client };
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("aw-server did not start on port {port}");
}

/// Write a peer staging db at `path` with one bucket and `n` events.
fn write_peer_db(path: &Path, bucket_id: &str, hostname: &str, n: usize, pad: usize) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let ds = create_datastore(path).unwrap();
    ds.create_bucket(&bucket(bucket_id, hostname)).unwrap();
    let base = Utc::now() - Duration::days(1);
    let padding = "x".repeat(pad);
    let events: Vec<Event> = (0..n)
        .map(|i| {
            let mut e = event(base + Duration::seconds(i as i64 * 10), 5, "a");
            e.data
                .insert("pad".into(), serde_json::json!(padding.clone()));
            e
        })
        .collect();
    ds.insert_events(bucket_id, &events).unwrap();
    ds.force_commit().unwrap();
    ds.close();
    // Give the worker a moment to drop the connection (checkpoints the WAL).
    std::thread::sleep(std::time::Duration::from_millis(300));
}

// ---------------------------------------------------------------------------
// Finding: a folder that merely *names* another device's id, anywhere in the
// sync dir, shadows that device's real db if its file is larger. pull_all
// then never imports the real peer.
// ---------------------------------------------------------------------------
#[test]
fn foreign_folder_claiming_peer_device_id_shadows_real_peer() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("shadow");
    isolate_env(&root);
    let sync = root.join("sync");
    std::env::set_var("AW_SYNC_DIR", &sync);

    // Honest alice at {sync}/alice/dev-alice/sync.db.
    write_peer_db(
        &sync.join("alice/dev-alice/sync.db"),
        "aw-watcher-window_alice",
        "alice",
        3,
        0,
    );
    // Mallory, in her own host folder, names her device dir "dev-alice" and
    // pads her db so it is larger.
    write_peer_db(
        &sync.join("mallory/dev-alice/sync.db"),
        "aw-watcher-window_mallory",
        "mallory",
        200,
        2000,
    );
    let a = std::fs::metadata(sync.join("alice/dev-alice/sync.db"))
        .unwrap()
        .len();
    let m = std::fs::metadata(sync.join("mallory/dev-alice/sync.db"))
        .unwrap()
        .len();
    eprintln!("alice db {a} bytes, mallory db {m} bytes");
    assert!(m > a, "precondition: mallory's db must be larger");

    let server = start_server(&root, "local-dev");
    let report = aw_sync::pull_all(&server.client).unwrap();
    eprintln!("pull_all report: {report}");
    let ids: Vec<String> = server.client.get_buckets().unwrap().into_keys().collect();
    eprintln!("local buckets after pull_all: {ids:?}");
    assert!(
        ids.iter()
            .any(|id| id == "aw-watcher-window_alice-synced-from-alice"),
        "alice's real db was skipped because another folder claimed her device_id: {ids:?}"
    );
}

// ---------------------------------------------------------------------------
// Finding: push opens its staging db through symlinks. A peer that replaces
// `{sync}/{our_device_id}/sync.db` with a symlink (Syncthing replicates
// symlinks on Unix) makes aw-sync open an arbitrary user-writable sqlite file
// read-write, run aw-datastore migrations on it and switch it to WAL.
// ---------------------------------------------------------------------------
#[test]
fn push_follows_planted_symlink_and_migrates_foreign_sqlite_db() {
    let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("symlink-push");
    isolate_env(&root);
    let sync = root.join("sync");
    let outside = root.join("outside");
    std::fs::create_dir_all(&outside).unwrap();

    // Another application's sqlite db outside the sync dir.
    let foreign = outside.join("places.sqlite");
    {
        let conn = rusqlite::Connection::open(&foreign).unwrap();
        conn.execute_batch(
            "CREATE TABLE moz_places(id INTEGER PRIMARY KEY, url TEXT);
             INSERT INTO moz_places(url) VALUES ('https://example.org');",
        )
        .unwrap();
    }

    // The planted symlink at our own staging path (daemon layout).
    let device_dir = sync.join("local-dev");
    std::fs::create_dir_all(&device_dir).unwrap();
    std::os::unix::fs::symlink(&foreign, device_dir.join("sync.db")).unwrap();

    let server = start_server(&root, "local-dev");
    server
        .client
        .create_bucket_simple("aw-watcher-window_local", "currentwindow")
        .unwrap();
    let spec = SyncSpec {
        path: sync.clone(),
        path_db: None,
        buckets: None,
        start: None,
    };
    let res = aw_sync::sync_run(&server.client, &spec, SyncMode::Push);
    eprintln!("push result: {:?}", res.as_ref().map(|r| r.to_string()));
    std::thread::sleep(std::time::Duration::from_millis(300));

    let conn =
        rusqlite::Connection::open_with_flags(&foreign, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap();
    let mode: String = conn
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .unwrap();
    eprintln!(
        "foreign db after push: tables={tables:?} user_version={version} journal_mode={mode}"
    );
    assert_eq!(
        (tables, version),
        (vec!["moz_places".to_string()], 0),
        "push wrote aw-sync schema into a file outside the sync dir via a symlink"
    );
}
