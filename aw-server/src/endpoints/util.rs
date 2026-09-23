use std::fs::File;
use std::io::{pipe, Cursor, PipeReader, PipeWriter};
use std::thread;

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
        let datastore = self.datastore;
        let bucket_id = self.bucket_id;
        thread::spawn(move || {
            if let Err(err) =
                datastore.export_to_file(bucket_id.as_deref(), pipe_writer_to_file(writer))
            {
                error!("Export stream failed: {err:?}");
            }
        });
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
