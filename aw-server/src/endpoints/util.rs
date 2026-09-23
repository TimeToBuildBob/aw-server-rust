use std::fs::File;
use std::io::{copy, pipe, Cursor, PipeReader, PipeWriter, Seek, SeekFrom, Write};
use std::thread;

use chrono::{DateTime, Utc};
use rocket::http::ContentType;
use rocket::http::Header;
use rocket::http::Status;
use rocket::request::Request;
use rocket::response::{self, Responder, Response};
use serde::Serialize;

#[derive(Serialize, Debug)]
pub struct HttpErrorJson {
    #[serde(skip_serializing)]
    status: Status,
    message: String,
}

impl HttpErrorJson {
    pub fn new(status: Status, err: String) -> HttpErrorJson {
        HttpErrorJson {
            status,
            message: err,
        }
    }
}

impl<'r> Responder<'r, 'static> for HttpErrorJson {
    fn respond_to(self, _: &Request) -> response::Result<'static> {
        let body = serde_json::to_string(&self).map_err(|err| {
            error!("Failed to serialize error response: {err}");
            Status::InternalServerError
        })?;
        Response::build()
            .status(self.status)
            .sized_body(body.len(), Cursor::new(body))
            .header(ContentType::new("application", "json"))
            .ok()
    }
}

pub struct BucketsExportRocket {
    datastore: aw_datastore::Datastore,
    bucket_id: Option<String>,
    filename: String,
}

fn export_filename(
    datastore: &aw_datastore::Datastore,
    bucket_id: Option<&str>,
) -> Result<String, HttpErrorJson> {
    let name = match bucket_id {
        Some(id) => {
            datastore.get_bucket(id)?;
            Some(id.to_owned())
        }
        None => {
            let buckets = datastore.get_buckets()?;
            (buckets.len() == 1).then(|| buckets.into_keys().next().unwrap())
        }
    };
    Ok(match name {
        Some(id) => format!("attachment; filename=aw-bucket-export_{id}.json"),
        None => "attachment; filename=aw-buckets-export.json".into(),
    })
}

#[cfg(not(any(unix, windows)))]
compile_error!("export streaming requires unix or windows anonymous pipes");

fn pipe_reader_to_file(reader: PipeReader) -> File {
    #[cfg(unix)]
    {
        File::from(std::os::fd::OwnedFd::from(reader))
    }
    #[cfg(windows)]
    {
        File::from(std::os::windows::io::OwnedHandle::from(reader))
    }
}

fn pipe_writer_to_file(writer: PipeWriter) -> File {
    #[cfg(unix)]
    {
        File::from(std::os::fd::OwnedFd::from(writer))
    }
    #[cfg(windows)]
    {
        File::from(std::os::windows::io::OwnedHandle::from(writer))
    }
}

/// Serialize on the datastore worker into a private tempfile, then copy to
/// the client pipe from this thread. The worker stays disk-paced; a slow
/// or dropped download must not stall heartbeats (see `ServerState`).
fn spawn_export_stream(
    datastore: aw_datastore::Datastore,
    bucket_id: Option<String>,
    writer: PipeWriter,
) {
    thread::spawn(move || {
        let staging = match tempfile::tempfile() {
            Ok(file) => file,
            Err(err) => {
                error!("Failed to create export staging file: {err}");
                return;
            }
        };
        let mut staging = match datastore.export_to_file(bucket_id.as_deref(), staging) {
            Ok((file, _)) => file,
            Err(err) => {
                error!("Export stream failed: {err:?}");
                return;
            }
        };
        if let Err(err) = staging.seek(SeekFrom::Start(0)) {
            error!("Failed to rewind export staging file: {err}");
            return;
        }
        let mut writer = pipe_writer_to_file(writer);
        if let Err(err) = copy(&mut staging, &mut writer) {
            error!("Export stream copy failed: {err}");
        }
    });
}

impl BucketsExportRocket {
    pub fn new(
        datastore: &aw_datastore::Datastore,
        bucket_id: Option<&str>,
    ) -> Result<Self, HttpErrorJson> {
        // Resolve the download name and 404 missing buckets before the
        // response is built. Serialization itself runs after headers so a
        // slow export does not look like a hung connection.
        let filename = export_filename(datastore, bucket_id)?;
        Ok(Self {
            datastore: datastore.clone(),
            bucket_id: bucket_id.map(str::to_owned),
            filename,
        })
    }
}

impl<'r> Responder<'r, 'static> for BucketsExportRocket {
    fn respond_to(self, _: &Request) -> response::Result<'static> {
        let (reader, writer) = pipe().map_err(|err| {
            error!("Failed to open export pipe: {err}");
            Status::InternalServerError
        })?;
        spawn_export_stream(self.datastore, self.bucket_id, writer);
        Response::build()
            .status(Status::Ok)
            .header(Header::new("Content-Disposition", self.filename))
            .header(ContentType::JSON)
            .streamed_body(rocket::tokio::fs::File::from_std(pipe_reader_to_file(
                reader,
            )))
            .ok()
    }
}

// ── CSV streaming export ──────────────────────────────────────────────────────

/// Minimal RFC-4180 CSV field escaper.
fn csv_escape(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

/// Serialize a slice of events as RFC-4180 CSV.
///
/// Columns: `id`, `timestamp`, `duration`, then all keys from the first
/// event's `data` map (same schema the webui uses for client-side CSV).
fn events_to_csv(events: &[aw_models::Event]) -> String {
    let data_keys: Vec<String> = events
        .first()
        .map(|e| e.data.keys().cloned().collect())
        .unwrap_or_default();

    let header: Vec<&str> = {
        let mut h = vec!["id", "timestamp", "duration"];
        for k in &data_keys {
            h.push(k.as_str());
        }
        h
    };

    let mut out = header
        .iter()
        .map(|s| csv_escape(s))
        .collect::<Vec<_>>()
        .join(",");
    out.push('\n');

    for event in events {
        let mut fields = vec![
            event.id.map(|i| i.to_string()).unwrap_or_default(),
            event.timestamp.to_rfc3339(),
            // duration is serialized as fractional seconds (same as JSON)
            format!("{:.9}", event.duration.num_milliseconds() as f64 / 1000.0),
        ];
        for key in &data_keys {
            let val = match event.data.get(key) {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(v) => v.to_string(),
                None => String::new(),
            };
            fields.push(val);
        }
        out.push_str(
            &fields
                .iter()
                .map(|s| csv_escape(s))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push('\n');
    }
    out
}

pub struct BucketEventsCsvRocket {
    datastore: aw_datastore::Datastore,
    bucket_id: String,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    limit: Option<u64>,
    filename: String,
}

impl BucketEventsCsvRocket {
    pub fn new(
        datastore: &aw_datastore::Datastore,
        bucket_id: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        limit: Option<u64>,
    ) -> Result<Self, HttpErrorJson> {
        // 404-check the bucket before headers are sent so errors return
        // a proper JSON body rather than truncating mid-stream.
        datastore.get_bucket(bucket_id)?;
        let filename = format!("attachment; filename=aw-events-export-{bucket_id}.csv");
        Ok(Self {
            datastore: datastore.clone(),
            bucket_id: bucket_id.to_owned(),
            start,
            end,
            limit,
            filename,
        })
    }
}

impl<'r> Responder<'r, 'static> for BucketEventsCsvRocket {
    fn respond_to(self, _: &Request) -> response::Result<'static> {
        let Self {
            datastore,
            bucket_id,
            start,
            end,
            limit,
            filename,
        } = self;
        let (reader, writer) = pipe().map_err(|err| {
            error!("Failed to open CSV export pipe: {err}");
            Status::InternalServerError
        })?;
        // Serialize on the datastore worker into a private tempfile, then copy
        // to the client pipe. Headers are sent immediately; the download starts
        // before the CSV is fully built.
        thread::spawn(move || {
            let events = match datastore.get_events(&bucket_id, start, end, limit) {
                Ok(e) => e,
                Err(err) => {
                    error!("CSV export: get_events failed: {err:?}");
                    return;
                }
            };
            let csv = events_to_csv(&events);
            let mut staging = match tempfile::tempfile() {
                Ok(f) => f,
                Err(err) => {
                    error!("Failed to create CSV staging file: {err}");
                    return;
                }
            };
            if let Err(err) = staging.write_all(csv.as_bytes()) {
                error!("CSV staging write failed: {err}");
                return;
            }
            if let Err(err) = staging.seek(SeekFrom::Start(0)) {
                error!("CSV staging rewind failed: {err}");
                return;
            }
            let mut writer = pipe_writer_to_file(writer);
            if let Err(err) = copy(&mut staging, &mut writer) {
                error!("CSV export copy failed: {err}");
            }
        });
        Response::build()
            .status(Status::Ok)
            .header(Header::new("Content-Disposition", filename))
            .header(ContentType::new("text", "csv"))
            .streamed_body(rocket::tokio::fs::File::from_std(pipe_reader_to_file(
                reader,
            )))
            .ok()
    }
}

use aw_datastore::DatastoreError;

impl From<DatastoreError> for HttpErrorJson {
    fn from(val: DatastoreError) -> Self {
        match val {
            DatastoreError::NoSuchBucket(bucket_id) => HttpErrorJson::new(
                Status::NotFound,
                format!("The requested bucket '{bucket_id}' does not exist"),
            ),
            DatastoreError::BucketAlreadyExists(bucket_id) => HttpErrorJson::new(
                Status::NotModified,
                format!("Bucket '{bucket_id}' already exists"),
            ),
            DatastoreError::NoSuchKey(key) => HttpErrorJson::new(
                Status::NotFound,
                format!("The requested key(s) '{key}' do not exist"),
            ),
            DatastoreError::MpscError => HttpErrorJson::new(
                Status::InternalServerError,
                "Unexpected Mpsc error!".to_string(),
            ),
            DatastoreError::InternalError(msg) => {
                HttpErrorJson::new(Status::InternalServerError, msg)
            }
            // When upgrade is disabled
            DatastoreError::Uninitialized(msg) => {
                HttpErrorJson::new(Status::InternalServerError, msg)
            }
            DatastoreError::OldDbVersion(msg) => {
                HttpErrorJson::new(Status::InternalServerError, msg)
            }
        }
    }
}
