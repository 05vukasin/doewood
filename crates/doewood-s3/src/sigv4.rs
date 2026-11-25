//! AWS Signature Version 4 in the `Authorization` header, for AWS JSON APIs (SQS).
//!
//! `rusty-s3` signs S3 requests as presigned URLs and fixes the service to `s3` in the
//! credential scope, so it cannot sign an SQS request. This is the same algorithm over the
//! same crypto crates (`hmac`, `sha2`, already in the tree through `rusty-s3`): a canonical
//! request with the payload hash, the string to sign, and the derived signing key. It signs
//! requests without a query string, which is all the JSON protocol sends.
//!
//! The signature and any session token are credentials: callers never log the headers.

use std::fmt::Write as _;

use doewood_store::S3Credentials;
use hmac::{Hmac, KeyInit as _, Mac as _};
use jiff::Timestamp;
use sha2::{Digest as _, Sha256};
use url::Url;

/// The headers to add to a request: `x-amz-date`, the session token if any, and
/// `authorization`. Lowercase names.
pub(crate) type AuthHeaders = Vec<(&'static str, String)>;

/// Signs a request. `headers` are the other headers sent and signed (lowercase names, such
/// as `content-type` and `x-amz-target`); `host` is taken from `url`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn sign(
    method: &str,
    url: &Url,
    headers: &[(&str, &str)],
    payload: &[u8],
    credentials: &S3Credentials,
    region: &str,
    service: &str,
    at: Timestamp,
) -> AuthHeaders {
    debug_assert!(
        url.query().is_none(),
        "only requests without a query are signed"
    );
    let amz_date = at.strftime("%Y%m%dT%H%M%SZ").to_string();
    let date = &amz_date[..8];
    let host = match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().to_owned(),
    };
    let token = credentials.session_token().filter(|t| !t.is_empty());
    let mut signed: Vec<(String, String)> = headers
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    signed.push(("host".to_owned(), host));
    signed.push(("x-amz-date".to_owned(), amz_date.clone()));
    if let Some(t) = token {
        signed.push(("x-amz-security-token".to_owned(), t.to_owned()));
    }
    signed.sort();
    let signed_names = signed
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let mut canonical = String::new();
    let path = if url.path().is_empty() {
        "/"
    } else {
        url.path()
    };
    let _ = write!(canonical, "{method}\n{path}\n\n");
    for (k, v) in &signed {
        let _ = writeln!(canonical, "{k}:{v}");
    }
    let _ = write!(
        canonical,
        "\n{signed_names}\n{}",
        hex(&Sha256::digest(payload))
    );
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let secret = format!("AWS4{}", credentials.secret_access_key());
    let key = hmac(secret.as_bytes(), date.as_bytes());
    let key = hmac(&key, region.as_bytes());
    let key = hmac(&key, service.as_bytes());
    let key = hmac(&key, b"aws4_request");
    let signature = hex(&hmac(&key, to_sign.as_bytes()));
    let mut out: AuthHeaders = vec![("x-amz-date", amz_date)];
    if let Some(t) = token {
        out.push(("x-amz-security-token", t.to_owned()));
    }
    out.push((
        "authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_names}, Signature={signature}",
            credentials.access_key_id()
        ),
    ));
    out
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The credentials and time of AWS's published SigV4 test suite.
    fn suite() -> (S3Credentials, Timestamp) {
        (
            S3Credentials::new(
                "AKIDEXAMPLE",
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                None,
            )
            .unwrap(),
            "2015-08-30T12:36:00Z".parse().unwrap(),
        )
    }

    fn authorization(h: &AuthHeaders) -> &str {
        &h.iter().find(|(k, _)| *k == "authorization").unwrap().1
    }

    #[test]
    fn matches_the_aws_test_suite() {
        let (creds, at) = suite();
        let url: Url = "https://example.amazonaws.com/".parse().unwrap();
        // get-vanilla
        let h = sign("GET", &url, &[], b"", &creds, "us-east-1", "service", at);
        assert_eq!(
            authorization(&h),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-date, \
             Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
        assert_eq!(h[0], ("x-amz-date", "20150830T123600Z".to_owned()));
        // post-vanilla
        let h = sign("POST", &url, &[], b"", &creds, "us-east-1", "service", at);
        assert!(authorization(&h).ends_with(
            "Signature=5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"
        ));
        // post-x-www-form-urlencoded: a signed content type and a payload.
        let h = sign(
            "POST",
            &url,
            &[("content-type", "application/x-www-form-urlencoded")],
            b"Param1=value1",
            &creds,
            "us-east-1",
            "service",
            at,
        );
        assert_eq!(
            authorization(&h),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date, \
             Signature=ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a"
        );
    }

    #[test]
    fn session_tokens_are_signed_and_sent() {
        let (_, at) = suite();
        let creds = S3Credentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("test-session-token".into()),
        )
        .unwrap();
        let url: Url = "http://127.0.0.1:9324/".parse().unwrap();
        let h = sign("POST", &url, &[], b"{}", &creds, "eu-west-1", "sqs", at);
        assert_eq!(
            h[1],
            ("x-amz-security-token", "test-session-token".to_owned())
        );
        let auth = authorization(&h);
        assert!(auth.contains("/eu-west-1/sqs/aws4_request"), "{auth}");
        assert!(
            auth.contains("SignedHeaders=host;x-amz-date;x-amz-security-token,"),
            "{auth}"
        );
        assert!(!auth.contains("test-session-token") && !auth.contains("wJalr"));
    }
}
