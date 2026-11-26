use aw_datastore::Datastore;
use aw_server::{config::AWConfig, endpoints};
use rocket::http::{ContentType, Header, Status};
use rocket::local::blocking::Client;
use serde_json::{json, Value};

fn client() -> Client {
    let state = endpoints::ServerState::new(
        Datastore::new_in_memory(false),
        endpoints::AssetResolver::new(None),
        "hunt-e264".to_string(),
    );
    Client::tracked(endpoints::build_rocket(state, AWConfig::default())).unwrap()
}

fn host() -> Header<'static> {
    Header::new("Host", "127.0.0.1:5600")
}

fn bucket() -> Value {
    json!({
        "id": "negative-duration", "type": "test", "client": "test", "hostname": "test",
        "events": [{"timestamp": "2026-01-01T00:00:05Z", "duration": -2, "data": {}}]
    })
}

fn assert_no_negative_events(client: &Client) {
    let response = client
        .get("/api/0/buckets/negative-duration/events")
        .header(host())
        .dispatch();
    if response.status() == Status::NotFound {
        return;
    }
    assert_eq!(response.status(), Status::Ok);
    let events: Value = response.into_json().unwrap();
    assert!(
        events
            .as_array()
            .unwrap()
            .iter()
            .all(|event| event["duration"].as_f64().unwrap() >= 0.0),
        "negative event persisted: {events}"
    );
}

#[test]
fn new_bucket_import_does_not_persist_negative_duration() {
    let client = client();
    let response = client
        .post("/api/0/import")
        .header(ContentType::JSON)
        .header(host())
        .body(json!({"buckets": {"negative-duration": bucket()}}).to_string())
        .dispatch();
    eprintln!("new-bucket import returned {}", response.status());
    assert_no_negative_events(&client);
}

#[test]
fn existing_bucket_import_does_not_persist_negative_duration() {
    let client = client();
    let mut metadata = bucket();
    metadata.as_object_mut().unwrap().remove("events");
    let response = client
        .post("/api/0/buckets/negative-duration")
        .header(ContentType::JSON)
        .header(host())
        .body(metadata.to_string())
        .dispatch();
    assert_eq!(response.status(), Status::Ok);
    let response = client
        .post("/api/0/import")
        .header(ContentType::JSON)
        .header(host())
        .body(json!({"buckets": {"negative-duration": bucket()}}).to_string())
        .dispatch();
    eprintln!("existing-bucket import returned {}", response.status());
    assert_no_negative_events(&client);
}

#[test]
fn bucket_create_does_not_persist_negative_duration() {
    let client = client();
    let response = client
        .post("/api/0/buckets/negative-duration")
        .header(ContentType::JSON)
        .header(host())
        .body(bucket().to_string())
        .dispatch();
    eprintln!("bucket create returned {}", response.status());
    assert_no_negative_events(&client);
}
