use std::fs::File;
use std::io::{copy, pipe, Cursor, PipeReader, PipeWriter, Seek, SeekFrom};
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

fn spawn_csv_export_stream(
    datastore: aw_datastore::Datastore,
    bucket_id: String,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    limit: Option<u64>,
    writer: PipeWriter,
) {
    thread::spawn(move || {
        let staging = match tempfile::tempfile() {
            Ok(file) => file,
            Err(err) => {
                error!("Failed to create CSV staging file: {err}");
                return;
            }
        };
        // Serialize on the datastore worker, one SQL row at a time, into the
        // staging file. The full event set is never materialized in memory,
        // and a flush failure (e.g. full staging filesystem) surfaces as an
        // error instead of a silently truncated CSV.
        let mut staging = match datastore.export_csv_to_file(&bucket_id, start, end, limit, staging)
        {
            Ok(file) => file,
            Err(err) => {
                error!("CSV export serialization failed: {err:?}");
                return;
            }
        };
        if let Err(err) = staging.seek(SeekFrom::Start(0)) {
            error!("CSV staging rewind failed: {err}");
            return;
        }
        let mut writer = pipe_writer_to_file(writer);
        if let Err(err) = copy(&mut staging, &mut writer) {
            error!("CSV export copy failed: {err}");
        }
    });
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
        // Resolve 404/500 before headers commit. get_bucket catches a missing
        // bucket; LIMIT 1 forces the same SQL the full export will run so a
        // down worker or a prepare/read failure still returns JSON instead of
        // a 200 with an empty CSV. Mid-stream failures after 200 cannot change
        // the status without delaying headers until serialization finishes —
        // that hung-connection behavior is what this endpoint exists to avoid
        // (same tradeoff as JSON export / #721).
        datastore.get_bucket(bucket_id)?;
        datastore.get_events(bucket_id, start, end, Some(1))?;
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
        spawn_csv_export_stream(datastore, bucket_id, start, end, limit, writer);
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
