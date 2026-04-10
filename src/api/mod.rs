mod error;
mod openapi;
mod route;

#[cfg(test)]
mod tests;

use std::{
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use http::{Request, Response, StatusCode};
use http_body::Body;
use parking_lot::RwLock;
use tower_service::Service;

use self::error::Error;
use crate::{ExtendedExecutor, Runtime, StorageBackend};

/// A service that exposes the BPMN runtime API over HTTP.
#[derive(Clone)]
pub struct Api<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(Arc<Inner<E, B>>);

struct Inner<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    entry_point: &'static str,
    runtime: RwLock<Runtime<E, B>>,
    root: route::Route<E, B>,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Api<E, B> {
    /// Creates a new API service with the given runtime.
    pub fn new(entry_point: &'static str, runtime: Runtime<E, B>) -> Self {
        let root = route::Route::from_engine(&runtime);
        Api(Arc::new(Inner {
            entry_point,
            runtime: RwLock::new(runtime),
            root,
        }))
    }

    async fn handle_request<Bo: Body + Send + 'static>(
        api_entry_point: &str,
        root: &route::Route<E, B>,
        runtime: &RwLock<Runtime<E, B>>,
        request: Request<Bo>,
    ) -> Result<Response<String>, Error>
    where
        Bo::Data: bytes::Buf + Send,
        Bo::Error: std::fmt::Display,
    {
        let (parts, body) = request.into_parts();
        let method = parts.method;
        let raw_segments = collect_path_segments(parts.uri.path());
        let segments = strip_entry_point(&raw_segments, api_entry_point);
        root.serve(segments, method, body, runtime).await
    }
}

fn collect_path_segments(path: &str) -> Vec<&str> {
    path.trim_matches('/')
        .split('/')
        .filter(|value| !value.is_empty())
        .collect()
}

fn strip_entry_point<'a>(segments: &'a [&'a str], entry_point: &str) -> &'a [&'a str] {
    segments
        .iter()
        .position(|value| *value == entry_point)
        .map_or(segments, |index| &segments[index + 1..])
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> std::fmt::Debug for Api<E, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api").finish_non_exhaustive()
    }
}

impl<E, B: Body, SB: StorageBackend> Service<Request<B>> for Api<E, SB>
where
    E: ExtendedExecutor<SB::Storage> + Send + Sync + 'static,
    B: Body + Send + 'static,
    B::Data: bytes::Buf + Send,
    B::Error: std::fmt::Display,
{
    type Response = Response<String>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let state = self.0.clone();
        Box::pin(async move {
            match Self::handle_request(state.entry_point, &state.root, &state.runtime, req).await {
                Ok(response) => Ok(response),
                Err(err) => Ok(err.into_response()),
            }
        })
    }
}

pub(super) fn json_response<T: serde::Serialize>(
    status: StatusCode,
    value: &T,
) -> Response<String> {
    let body = serde_json::to_string(value)
        .unwrap_or_else(|err| format!(r#"{{"error":"failed to serialize response: {err}"}}"#));

    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| Response::new("{\"error\":\"failed to build response\"}".to_string()))
}
