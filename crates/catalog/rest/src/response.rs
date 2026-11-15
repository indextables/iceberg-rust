// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The response type REST catalog requests come back as.

use std::fmt::{Debug, Formatter};

use http::{HeaderMap, StatusCode};
use iceberg::Result;
use reqwest::Response;

use crate::client::format_headers_redacted;

/// A REST catalog response, read into memory.
///
/// The counterpart of [`HttpRequest`](crate::HttpRequest): it keeps the
/// concrete client type inside [`HttpClient`](crate::HttpClient) so callers
/// work with the stable `http` crate types instead. Catalog responses are
/// small JSON documents that every caller reads in full, so the body is
/// buffered rather than streamed.
#[derive(Clone)]
pub struct HttpResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Debug for HttpResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // A token exchange answers with a credential in the body, and headers
        // may carry `set-cookie`, so both are held back.
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            // Always redacted: a response carries no config, and the
            // `disable-header-redaction` escape hatch is for the request side.
            .field("headers", &format_headers_redacted(&self.headers, false))
            .field("body", &format_args!("{} bytes", self.body.len()))
            .finish()
    }
}

impl HttpResponse {
    /// Reads `response` into memory, failing before buffering more than
    /// `max_bytes` of its body when a limit is given.
    pub(crate) async fn read(mut response: Response, max_bytes: Option<u64>) -> Result<Self> {
        let status = response.status();
        let headers = response.headers().clone();
        // `bytes()` and `chunk()` build their errors without a URL, so keep
        // the one the response came from.
        let url = response.url().clone();
        let too_long = |limit: u64| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                format!("The rest catalog response body exceeds the limit of {limit} bytes"),
            )
            .with_context("status", status.to_string())
        };
        let Some(limit) = max_bytes else {
            return Ok(Self {
                status,
                headers,
                body: response
                    .bytes()
                    .await
                    .map_err(|err| err.with_url(url))?
                    .to_vec(),
            });
        };
        if response.content_length().is_some_and(|len| len > limit) {
            return Err(too_long(limit));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|err| err.with_url(url.clone()))?
        {
            if body.len() as u64 + chunk.len() as u64 > limit {
                return Err(too_long(limit));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Self {
            status,
            headers,
            body,
        })
    }

    /// Builds a response, e.g. to unit-test code that consumes one.
    pub fn new(status: StatusCode, headers: HeaderMap, body: Vec<u8>) -> Self {
        Self {
            status,
            headers,
            body,
        }
    }

    /// The response status.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// The response headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// The response body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reqwest_response(body: &'static str) -> Response {
        Response::from(http::Response::builder().status(200).body(body).unwrap())
    }

    #[tokio::test]
    async fn test_read_within_the_limit_keeps_the_body() {
        let r = HttpResponse::read(reqwest_response("{\"a\":1}"), Some(7)).await.unwrap();
        assert_eq!(r.body(), b"{\"a\":1}");
        let r = HttpResponse::read(reqwest_response("{\"a\":1}"), None).await.unwrap();
        assert_eq!(r.body().len(), 7);
    }

    #[tokio::test]
    async fn test_read_above_the_limit_fails_naming_it() {
        let err = HttpResponse::read(reqwest_response("{\"a\":12}"), Some(7)).await.unwrap_err();
        assert!(err.to_string().contains("limit of 7 bytes"), "{err}");
    }

    #[test]
    fn test_debug_holds_back_secrets() {
        // The body of a token exchange is a credential, and `set-cookie` is
        // redacted everywhere else in this crate.
        let mut headers = HeaderMap::new();
        headers.insert("set-cookie", "session=secret".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let debug = format!(
            "{:?}",
            HttpResponse::new(
                StatusCode::OK,
                headers,
                br#"{"access_token": "tok"}"#.to_vec()
            )
        );

        assert!(!debug.contains("secret"), "{debug}");
        assert!(!debug.contains("tok"), "{debug}");
        assert!(debug.contains("content-type"), "{debug}");
        assert!(debug.contains("200"), "{debug}");
    }

    #[test]
    fn test_new_exposes_what_it_was_built_from() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        let response = HttpResponse::new(StatusCode::OK, headers, "{}".as_bytes().to_vec());

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/json"
        );
        assert_eq!(response.body(), b"{}");
    }
}
