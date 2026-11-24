//! A light S3 client: `rusty-s3` presigns each request with SigV4 (Sans-IO, no runtime of its
//! own) and `reqwest` (HTTP/1.1, rustls) sends it. It speaks to any S3-compatible endpoint:
//! AWS, MinIO, Ceph, R2, Wasabi; path-style or virtual-host-style; any region; optional session
//! tokens.
//!
//! Presigned URLs carry the access key id, the signature and any session token, so they never
//! leave this module: they are not logged and errors do not carry them.

use std::fmt;
use std::sync::Once;
use std::time::Duration;

use bytes::Bytes;
use doewood_store::{S3Credentials, Source, UrlStyle};
use jiff::Timestamp;
use rusty_s3::actions::ListObjectsV2;
use rusty_s3::{Bucket, Credentials, S3Action};
use tracing::debug;
use url::Url;

use crate::S3Error;
use crate::error::transport;
use crate::retry::{RetryPolicy, retryable_status};

/// Largest error body read to find the S3 error code.
const ERROR_BODY_MAX: usize = 16 * 1024;

/// Timeouts and retries of a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientOptions {
    /// Time to establish a connection (TCP and TLS).
    pub connect_timeout: Duration,
    /// Time for a whole `ListObjectsV2` or `HeadObject` request.
    pub request_timeout: Duration,
    /// Longest pause between two reads of a response body: a stalled GetObject fails after this,
    /// however large the object.
    pub read_timeout: Duration,
    /// Retries on throttling, server errors and transport failures.
    pub retry: RetryPolicy,
    /// Validity of each presigned request; S3 checks it when the request starts.
    pub presign_expiry: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_mins(1),
            read_timeout: Duration::from_mins(1),
            retry: RetryPolicy::default(),
            presign_expiry: Duration::from_mins(15),
        }
    }
}

/// Where and how to reach a bucket.
#[derive(Clone, Debug)]
pub struct S3Config {
    /// Endpoint URL, such as `https://s3.eu-central-1.amazonaws.com` or `http://minio:9000`.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Signing region (`us-east-1` for MinIO and Ceph unless configured otherwise; `auto` for R2).
    pub region: String,
    /// Path-style or virtual-host-style URLs.
    pub url_style: UrlStyle,
    /// Credentials; `None` sends unsigned requests (public buckets).
    pub credentials: Option<S3Credentials>,
}

impl S3Config {
    /// The configuration of a stored source, with its credentials from the store.
    pub fn for_source(source: &Source, credentials: Option<S3Credentials>) -> Self {
        Self {
            endpoint: source.endpoint.clone(),
            bucket: source.bucket.clone(),
            region: source.region().to_owned(),
            url_style: source.url_style,
            credentials,
        }
    }
}

/// One `ListObjectsV2` request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ListRequest<'a> {
    /// Only keys starting with this.
    pub prefix: &'a str,
    /// Only keys after this one (used to resume; ignored by S3 with a continuation token).
    pub start_after: Option<&'a str>,
    /// The token of the previous page.
    pub continuation_token: Option<&'a str>,
    /// Keys per page; S3 returns at most 1000.
    pub max_keys: usize,
}

/// An object in a listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedObject {
    /// Its key.
    pub key: String,
    /// Its ETag, without quotes.
    pub etag: Option<String>,
    /// Its size in bytes.
    pub size: u64,
    /// Its last-modified time (Unix seconds).
    pub last_modified: Option<i64>,
}

/// A page of a listing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListPage {
    /// The objects, in key order.
    pub objects: Vec<ListedObject>,
    /// The token for the next page; `None` on the last page.
    pub next_continuation_token: Option<String>,
}

/// An object's metadata, from `HeadObject` or a GetObject response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Size in bytes (`Content-Length`).
    pub size: u64,
    /// ETag, without quotes.
    pub etag: Option<String>,
    /// Last-modified time (Unix seconds).
    pub last_modified: Option<i64>,
    /// `Content-Type`.
    pub content_type: Option<String>,
}

/// An S3 client for one bucket.
#[derive(Clone)]
pub struct S3Client {
    http: reqwest::Client,
    bucket: Bucket,
    credentials: Option<Credentials>,
    options: ClientOptions,
}

impl fmt::Debug for S3Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Client")
            .field("bucket", &self.bucket.name())
            .field("region", &self.bucket.region())
            .field("signed", &self.credentials.is_some())
            .finish_non_exhaustive()
    }
}

/// Builds the HTTP client every S3 client of the process can share (a connection pool).
pub fn http_client(options: &ClientOptions) -> Result<reqwest::Client, S3Error> {
    static PROVIDER: Once = Once::new();
    PROVIDER.call_once(|| {
        // Another component may have installed one already; either is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
    reqwest::Client::builder()
        .connect_timeout(options.connect_timeout)
        .read_timeout(options.read_timeout)
        .user_agent(concat!("doewood/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| transport("client setup", e))
}

impl S3Client {
    /// A client with its own HTTP connection pool.
    pub fn new(config: &S3Config, options: ClientOptions) -> Result<Self, S3Error> {
        Self::with_http(config, options, http_client(&options)?)
    }

    /// A client on a shared HTTP client (see [`http_client`]).
    pub fn with_http(
        config: &S3Config,
        options: ClientOptions,
        http: reqwest::Client,
    ) -> Result<Self, S3Error> {
        let endpoint: Url = config
            .endpoint
            .parse()
            .map_err(|e| S3Error::Config(format!("endpoint is not a URL: {e}")))?;
        if !endpoint.username().is_empty() || endpoint.password().is_some() {
            return Err(S3Error::Config(
                "the endpoint must not contain credentials".to_owned(),
            ));
        }
        let style = match config.url_style {
            UrlStyle::Path => rusty_s3::UrlStyle::Path,
            UrlStyle::VirtualHost => rusty_s3::UrlStyle::VirtualHost,
        };
        let bucket = Bucket::new(
            endpoint,
            style,
            config.bucket.clone(),
            config.region.clone(),
        )
        .map_err(|e| S3Error::Config(e.to_string()))?;
        let credentials = config
            .credentials
            .as_ref()
            .map(|c| match c.session_token() {
                Some(token) => {
                    Credentials::new_with_token(c.access_key_id(), c.secret_access_key(), token)
                }
                None => Credentials::new(c.access_key_id(), c.secret_access_key()),
            });
        Ok(Self {
            http,
            bucket,
            credentials,
            options,
        })
    }

    /// The bucket name.
    pub fn bucket(&self) -> &str {
        self.bucket.name()
    }

    /// One page of `ListObjectsV2`.
    pub async fn list_objects(&self, req: &ListRequest<'_>) -> Result<ListPage, S3Error> {
        const OP: &str = "ListObjectsV2";
        let resp = self
            .send(OP, Some(self.options.request_timeout), || {
                let mut action = self.bucket.list_objects_v2(self.credentials.as_ref());
                if !req.prefix.is_empty() {
                    action.with_prefix(req.prefix);
                }
                if let Some(token) = req.continuation_token {
                    action.with_continuation_token(token);
                } else if let Some(after) = req.start_after {
                    action.with_start_after(after);
                }
                if req.max_keys > 0 {
                    action.with_max_keys(req.max_keys);
                }
                (reqwest::Method::GET, self.sign(&action, None))
            })
            .await?;
        let body = resp.text().await.map_err(|e| transport(OP, e))?;
        parse_list(&body)
    }

    /// `HeadObject`; `None` if there is no such key.
    pub async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, S3Error> {
        const OP: &str = "HeadObject";
        let result = self
            .send(OP, Some(self.options.request_timeout), || {
                let action = self.bucket.head_object(self.credentials.as_ref(), key);
                (reqwest::Method::HEAD, self.sign(&action, None))
            })
            .await;
        match result {
            Ok(resp) => Ok(Some(meta(&resp))),
            Err(e) if e.status() == Some(404) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// GetObject, streamed. Fails with [`S3Error::TooLarge`] before reading when the object is
    /// larger than `max_bytes`, and while reading if the server sends more than that.
    pub async fn get_object(&self, key: &str, max_bytes: u64) -> Result<ObjectBody, S3Error> {
        const OP: &str = "GetObject";
        // No whole-request timeout: a large object takes as long as it takes. The read timeout
        // catches a stalled transfer.
        let response = self
            .send(OP, None, || {
                let action = self.bucket.get_object(self.credentials.as_ref(), key);
                (reqwest::Method::GET, self.sign(&action, None))
            })
            .await?;
        let meta = meta(&response);
        if response.content_length().is_some_and(|n| n > max_bytes) {
            return Err(S3Error::TooLarge { limit: max_bytes });
        }
        Ok(ObjectBody {
            meta,
            response,
            received: 0,
            limit: max_bytes,
        })
    }

    fn sign<'a>(&self, action: &impl S3Action<'a>, at: Option<&Timestamp>) -> Url {
        match at {
            Some(at) => action.sign_with_time(self.options.presign_expiry, at),
            None => action.sign(self.options.presign_expiry),
        }
    }

    /// Sends a request, signing it afresh for every attempt, and retries what is worth it.
    async fn send(
        &self,
        op: &'static str,
        timeout: Option<Duration>,
        build: impl Fn() -> (reqwest::Method, Url),
    ) -> Result<reqwest::Response, S3Error> {
        let policy = self.options.retry;
        let mut attempt = 1_u32;
        loop {
            let (method, url) = build();
            let mut request = self.http.request(method.clone(), url);
            if let Some(t) = timeout {
                request = request.timeout(t);
            }
            let (error, retry_after) = match request.send().await {
                Ok(resp) if resp.status().is_success() => return Ok(resp),
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let retry_after = resp
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.trim().parse::<u64>().ok())
                        .map(Duration::from_secs);
                    let (code, message) = if method == reqwest::Method::HEAD {
                        (None, None)
                    } else {
                        parse_error(&read_capped(resp).await)
                    };
                    let retry = retryable_status(status, code.as_deref());
                    let error = S3Error::Status {
                        op,
                        status,
                        code,
                        message,
                    };
                    if !retry {
                        return Err(error);
                    }
                    (error, retry_after)
                }
                Err(e) if e.is_builder() => return Err(transport(op, e)),
                Err(e) => (transport(op, e), None),
            };
            if attempt >= policy.max_attempts {
                return Err(error);
            }
            let delay = policy.delay(attempt, retry_after);
            debug!(op, attempt, delay_ms = delay.as_millis(), error = %error, "retrying");
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }
}

/// A GetObject body, read chunk by chunk under the size cap.
#[derive(Debug)]
pub struct ObjectBody {
    meta: ObjectMeta,
    response: reqwest::Response,
    received: u64,
    limit: u64,
}

impl ObjectBody {
    /// The object's metadata from the response headers.
    pub fn meta(&self) -> &ObjectMeta {
        &self.meta
    }

    /// The next chunk; `None` at the end.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>, S3Error> {
        let chunk = self
            .response
            .chunk()
            .await
            .map_err(|e| transport("GetObject", e))?;
        if let Some(c) = &chunk {
            self.received += c.len() as u64;
            if self.received > self.limit {
                return Err(S3Error::TooLarge { limit: self.limit });
            }
        }
        Ok(chunk)
    }

    /// Reads the whole body into memory (still under the cap).
    pub async fn bytes(mut self) -> Result<Vec<u8>, S3Error> {
        let mut out =
            Vec::with_capacity(usize::try_from(self.meta.size.min(self.limit)).unwrap_or(0));
        while let Some(c) = self.chunk().await? {
            out.extend_from_slice(&c);
        }
        Ok(out)
    }
}

/// Reads at most [`ERROR_BODY_MAX`] bytes of an error body.
async fn read_capped(mut resp: reqwest::Response) -> String {
    let mut body = Vec::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        body.extend_from_slice(&chunk);
        if body.len() >= ERROR_BODY_MAX {
            body.truncate(ERROR_BODY_MAX);
            break;
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

/// The `Code` and `Message` of an S3 error body. Nothing else is kept: other elements can echo
/// the request (`StringToSign`, `AWSAccessKeyId`).
fn parse_error(body: &str) -> (Option<String>, Option<String>) {
    let element = |name: &str| {
        let open = format!("<{name}>");
        let start = body.find(&open)? + open.len();
        let end = start + body[start..].find(&format!("</{name}>"))?;
        let text = body[start..end].trim();
        (!text.is_empty()).then(|| text.chars().take(300).collect::<String>())
    };
    (element("Code"), element("Message"))
}

fn parse_list(body: &str) -> Result<ListPage, S3Error> {
    let parsed = ListObjectsV2::parse_response(body).map_err(|e| S3Error::Response {
        op: "ListObjectsV2",
        reason: e.to_string(),
    })?;
    let objects = parsed
        .contents
        .into_iter()
        .map(|c| ListedObject {
            key: c.key,
            etag: c.etag.as_deref().map(unquote),
            size: c.size.unwrap_or(0),
            last_modified: c
                .last_modified
                .as_deref()
                .and_then(|t| t.parse::<Timestamp>().ok())
                .map(Timestamp::as_second),
        })
        .collect();
    Ok(ListPage {
        objects,
        next_continuation_token: parsed.next_continuation_token,
    })
}

fn meta(resp: &reqwest::Response) -> ObjectMeta {
    let header = |name: reqwest::header::HeaderName| {
        resp.headers()
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    ObjectMeta {
        size: header(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        etag: header(reqwest::header::ETAG).as_deref().map(unquote),
        last_modified: header(reqwest::header::LAST_MODIFIED)
            .and_then(|v| jiff::fmt::rfc2822::parse(&v).ok())
            .map(|z| z.timestamp().as_second()),
        content_type: header(reqwest::header::CONTENT_TYPE),
    }
}

/// An ETag without its quotes.
pub(crate) fn unquote(etag: &str) -> String {
    etag.trim().trim_matches('"').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS's published example credentials (not a real account), from the SigV4 documentation.
    fn aws_example(url_style: UrlStyle, token: Option<&str>) -> S3Client {
        let config = S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            bucket: "examplebucket".into(),
            region: "us-east-1".into(),
            url_style,
            credentials: Some(
                S3Credentials::new(
                    "AKIAIOSFODNN7EXAMPLE",
                    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                    token.map(str::to_owned),
                )
                .unwrap(),
            ),
        };
        let options = ClientOptions {
            presign_expiry: Duration::from_hours(24),
            ..ClientOptions::default()
        };
        S3Client::new(&config, options).unwrap()
    }

    fn at_20130524() -> Timestamp {
        "2013-05-24T00:00:00Z".parse().unwrap()
    }

    #[test]
    fn presigned_get_matches_the_aws_sigv4_example() {
        // "Authenticating Requests: Using Query Parameters (AWS Signature Version 4)", the GET
        // of `test.txt` valid for 86400 s from 20130524T000000Z.
        let client = aws_example(UrlStyle::VirtualHost, None);
        let action = client
            .bucket
            .get_object(client.credentials.as_ref(), "test.txt");
        let url = client.sign(&action, Some(&at_20130524()));
        assert_eq!(
            url.as_str(),
            "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host\
             &X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    #[test]
    fn path_style_and_session_tokens_are_signed() {
        let client = aws_example(UrlStyle::Path, Some("example-session-token"));
        let action = client
            .bucket
            .head_object(client.credentials.as_ref(), "a b/c.pdf");
        let url = client.sign(&action, Some(&at_20130524()));
        assert_eq!(url.host_str(), Some("s3.amazonaws.com"));
        assert_eq!(url.path(), "/examplebucket/a%20b/c.pdf");
        let query: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let get = |k: &str| query.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
        assert_eq!(get("X-Amz-Security-Token"), Some("example-session-token"));
        assert_eq!(get("X-Amz-Signature").map(str::len), Some(64));
        // A different region signs differently.
        let mut other = aws_example(UrlStyle::Path, Some("example-session-token"));
        other.bucket = Bucket::new(
            "https://s3.amazonaws.com".parse().unwrap(),
            rusty_s3::UrlStyle::Path,
            "examplebucket",
            "eu-central-1",
        )
        .unwrap();
        let action = other
            .bucket
            .head_object(other.credentials.as_ref(), "a b/c.pdf");
        let url2 = other.sign(&action, Some(&at_20130524()));
        assert_ne!(url.as_str(), url2.as_str());
        assert!(url2.as_str().contains("eu-central-1"));
    }

    #[test]
    fn configuration_is_checked_and_debug_hides_credentials() {
        let client = aws_example(UrlStyle::Path, None);
        let debug = format!("{client:?}");
        assert!(
            !debug.contains("AKIA") && !debug.contains("EXAMPLEKEY"),
            "{debug}"
        );
        let bad = |endpoint: &str, style| S3Config {
            endpoint: endpoint.into(),
            bucket: "b".into(),
            region: "us-east-1".into(),
            url_style: style,
            credentials: None,
        };
        for config in [
            bad("not a url", UrlStyle::Path),
            bad("ftp://host", UrlStyle::Path),
            bad("http://user:pw@host", UrlStyle::Path),
            // Virtual-host style needs a DNS name.
            bad("http://127.0.0.1:9000", UrlStyle::VirtualHost),
        ] {
            assert!(
                matches!(
                    S3Client::new(&config, ClientOptions::default()),
                    Err(S3Error::Config(_))
                ),
                "{config:?}"
            );
        }
        // An endpoint path is kept as a prefix (an S3 gateway behind a path).
        let c = S3Client::new(
            &bad("http://gw.local/s3", UrlStyle::Path),
            ClientOptions::default(),
        )
        .unwrap();
        let action = c.bucket.get_object(None, "k");
        assert_eq!(c.sign(&action, None).as_str(), "http://gw.local/s3/b/k");
    }

    #[test]
    fn list_responses_parse() {
        let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>b</Name><Prefix>in/</Prefix><KeyCount>2</KeyCount><MaxKeys>2</MaxKeys>
  <EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>
  <Contents><Key>in/a+b%2Bc.pdf</Key><LastModified>2026-01-02T03:04:05.000Z</LastModified>
    <ETag>&quot;0123abcd&quot;</ETag><Size>42</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>in/%C3%A9.txt</Key><LastModified>2026-01-02T03:04:06.000Z</LastModified>
    <ETag>"ff-2"</ETag><Size>0</Size></Contents>
  <NextContinuationToken>tok/1==</NextContinuationToken>
</ListBucketResult>"#;
        let page = parse_list(body).unwrap();
        assert_eq!(page.next_continuation_token.as_deref(), Some("tok/1=="));
        assert_eq!(
            page.objects[0],
            ListedObject {
                key: "in/a b+c.pdf".into(),
                etag: Some("0123abcd".into()),
                size: 42,
                last_modified: Some(1_767_323_045),
            }
        );
        assert_eq!(page.objects[1].key, "in/é.txt");
        assert_eq!(page.objects[1].etag.as_deref(), Some("ff-2"));
        let last = body.replace(
            "<IsTruncated>true</IsTruncated>",
            "<IsTruncated>false</IsTruncated>",
        );
        assert_eq!(parse_list(&last).unwrap().next_continuation_token, None);
        assert!(parse_list("<nope/>").is_err());
    }

    #[test]
    fn error_bodies_keep_only_code_and_message() {
        let body = "<?xml version=\"1.0\"?><Error><Code>SignatureDoesNotMatch</Code>\
            <Message>The request signature we calculated does not match</Message>\
            <AWSAccessKeyId>AKIAIOSFODNN7EXAMPLE</AWSAccessKeyId>\
            <StringToSign>AWS4-HMAC-SHA256 ...</StringToSign></Error>";
        let (code, message) = parse_error(body);
        assert_eq!(code.as_deref(), Some("SignatureDoesNotMatch"));
        assert!(message.unwrap().starts_with("The request signature"));
        assert_eq!(parse_error(""), (None, None));
        let e = S3Error::Status {
            op: "ListObjectsV2",
            status: 503,
            code: Some("SlowDown".into()),
            message: Some("Please reduce your request rate.".into()),
        };
        assert_eq!(
            e.to_string(),
            "S3 ListObjectsV2 failed: HTTP 503 SlowDown: Please reduce your request rate."
        );
    }
}
