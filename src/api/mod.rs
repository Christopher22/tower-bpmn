//! HTTP API service wrapping the BPMN runtime.

mod error;
pub mod guards;
mod openapi;
mod route;

#[cfg(test)]
mod tests;

use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use http::{Request, Response, StatusCode};
use http_body::Body;
use parking_lot::RwLock;
use serde::Serialize;
use tower_service::Service;

use self::error::Error;
use self::guards::{EverybodyGuard, Guard};
use crate::bpmn::{
    ExtendedExecutor, MetaData, Process, Runtime,
    messages::{Context as MessageContext, Participant},
    storage::StorageBackend,
};

/// A service that exposes the BPMN runtime API over HTTP.
#[derive(Clone)]
pub struct Api<E: ExtendedExecutor<B::Storage>, B: StorageBackend, G: Guard = EverybodyGuard>(
    Arc<Inner<E, B, G>>,
);

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend, G: Guard> Api<E, B, G> {
    /// Returns a reference to the runtime wrapped by this API.
    pub fn runtime(&self) -> impl std::ops::Deref<Target = Runtime<E, B>> {
        self.0.runtime.read()
    }
}

struct Inner<E: ExtendedExecutor<B::Storage>, B: StorageBackend, G: Guard> {
    entry_point: &'static str,
    runtime: RwLock<Runtime<E, B>>,
    root: route::Route<E, B>,
    guard: G,
}

/// Builder for composing default and custom API routes.
pub struct ApiBuilder<E: ExtendedExecutor<B::Storage>, B: StorageBackend, G: Guard> {
    entry_point: &'static str,
    runtime: Runtime<E, B>,
    routes: route::RouteBuilder<E, B>,
    expose_openapi: bool,
    guard: G,
    exposed_processes: HashMap<MetaData, Participant>,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend, G: Guard> std::fmt::Debug
    for ApiBuilder<E, B, G>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiBuilder").finish_non_exhaustive()
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> ApiBuilder<E, B, EverybodyGuard> {
    fn new(entry_point: &'static str, runtime: Runtime<E, B>) -> Self {
        let mut routes = route::RouteBuilder::new();
        route::register_runtime_routes(&mut routes, &runtime);
        Self {
            entry_point,
            runtime,
            routes,
            expose_openapi: true,
            guard: EverybodyGuard,
            exposed_processes: HashMap::new(),
        }
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend, G: Guard> ApiBuilder<E, B, G> {
    /// Replaces the request guard used for deriving message context and authorization.
    pub fn with_guard<NG>(self, guard: NG) -> ApiBuilder<E, B, NG>
    where
        NG: Guard,
    {
        ApiBuilder {
            entry_point: self.entry_point,
            runtime: self.runtime,
            routes: self.routes,
            expose_openapi: self.expose_openapi,
            guard,
            exposed_processes: self.exposed_processes,
        }
    }

    /// Enables or disables automatic OpenAPI endpoint generation at path '/'.
    pub fn with_openapi(mut self, enabled: bool) -> Self {
        self.expose_openapi = enabled;
        self
    }

    pub fn with_exposed_steps<P: Process>(
        mut self,
        process: P,
        allowed_participant: Participant,
    ) -> Self {
        // By default, all processes are not exposed, so we only need to register explicitly exposed processes here.
        if allowed_participant != Participant::Nobody {
            self.exposed_processes
                .insert(process.metadata().clone(), allowed_participant);
        }
        self
    }

    /// Registers a custom typed GET endpoint.
    pub fn add_get<O, F>(mut self, path: &str, callback: F) -> Self
    where
        O: Serialize,
        F: Fn(&Runtime<E, B>, &MessageContext) -> Result<O, Error> + Send + Sync + 'static,
    {
        self.routes.add_get_typed(path, callback);
        self
    }

    /// Registers a custom typed GET endpoint with explicit participant requirement.
    pub fn add_get_for<O, F>(mut self, path: &str, participant: Participant, callback: F) -> Self
    where
        O: Serialize,
        F: Fn(&Runtime<E, B>, &MessageContext) -> Result<O, Error> + Send + Sync + 'static,
    {
        self.routes.add_get_typed_for(path, participant, callback);
        self
    }

    /// Registers a custom JSON GET endpoint.
    pub fn add_get_json<F>(mut self, path: &str, status: StatusCode, callback: F) -> Self
    where
        F: Fn(&Runtime<E, B>, &MessageContext) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        self.routes.add_get_json(path, status, callback);
        self
    }

    /// Registers a custom JSON GET endpoint with explicit participant requirement.
    pub fn add_get_json_for<F>(
        mut self,
        path: &str,
        participant: Participant,
        status: StatusCode,
        callback: F,
    ) -> Self
    where
        F: Fn(&Runtime<E, B>, &MessageContext) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        self.routes
            .add_get_json_for(path, participant, status, callback);
        self
    }

    /// Registers a custom JSON POST endpoint.
    pub fn add_post_json<F>(mut self, path: &str, status: StatusCode, callback: F) -> Self
    where
        F: Fn(
                &Runtime<E, B>,
                serde_json::Value,
                &MessageContext,
            ) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        self.routes.add_post_json(path, status, callback);
        self
    }

    /// Registers a custom JSON POST endpoint with explicit participant requirement.
    pub fn add_post_json_for<F>(
        mut self,
        path: &str,
        participant: Participant,
        status: StatusCode,
        callback: F,
    ) -> Self
    where
        F: Fn(
                &Runtime<E, B>,
                serde_json::Value,
                &MessageContext,
            ) -> Result<serde_json::Value, Error>
            + Send
            + Sync
            + 'static,
    {
        self.routes
            .add_post_json_for(path, participant, status, callback);
        self
    }

    /// Finalizes the API service.
    pub fn build(mut self) -> Api<E, B, G> {
        route::register_exposed_step_query_routes(
            &mut self.routes,
            &self.runtime,
            &self.exposed_processes,
        );

        let security_scheme = self.guard.openapi_security_scheme();
        if self.expose_openapi {
            // Register a placeholder first so '/' appears in the generated document itself.
            route::register_openapi_root(&mut self.routes, serde_json::json!({}));
            let first_pass_root = self.routes.clone().build();
            let openapi_document = openapi::build(&first_pass_root, security_scheme.clone());
            // Second registration replaces the placeholder callback while keeping route docs intact.
            route::register_openapi_root(&mut self.routes, openapi_document);
        }

        let root = self.routes.build();
        Api(Arc::new(Inner {
            entry_point: self.entry_point,
            runtime: RwLock::new(self.runtime),
            root,
            guard: self.guard,
        }))
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Api<E, B, EverybodyGuard> {
    /// Creates a builder for fine-grained route registration.
    pub fn builder(
        entry_point: &'static str,
        runtime: Runtime<E, B>,
    ) -> ApiBuilder<E, B, EverybodyGuard> {
        ApiBuilder::new(entry_point, runtime)
    }

    /// Creates a new API service with default runtime routes and OpenAPI at '/'.
    pub fn new(entry_point: &'static str, runtime: Runtime<E, B>) -> Self {
        Self::builder(entry_point, runtime).build()
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend, G: Guard> Api<E, B, G> {
    async fn handle_request<Bo: Body + Send + 'static>(
        api_entry_point: &str,
        root: &route::Route<E, B>,
        runtime: &RwLock<Runtime<E, B>>,
        guard: &G,
        request: Request<Bo>,
    ) -> Result<Response<String>, Error>
    where
        Bo::Data: bytes::Buf + Send,
        Bo::Error: std::fmt::Display,
    {
        let (parts, body) = request.into_parts();
        let context = guard.context_from_request(&parts);
        let method = parts.method;
        let raw_segments = collect_path_segments(parts.uri.path());
        let segments = strip_entry_point(&raw_segments, api_entry_point);
        root.serve(segments, method, body, runtime, &context).await
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

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend, G: Guard> std::fmt::Debug
    for Api<E, B, G>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api").finish_non_exhaustive()
    }
}

impl<E, B: Body, SB: StorageBackend, G: Guard> Service<Request<B>> for Api<E, SB, G>
where
    E: ExtendedExecutor<SB::Storage> + Send + Sync + 'static,
    G: Send + Sync + 'static,
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
            match Self::handle_request(
                state.entry_point,
                &state.root,
                &state.runtime,
                &state.guard,
                req,
            )
            .await
            {
                Ok(response) => Ok(response),
                Err(err) => Ok(err.into_response()),
            }
        })
    }
}

pub(super) fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Response<String> {
    let body = serde_json::to_string(value)
        .unwrap_or_else(|err| format!(r#"{{"error":"failed to serialize response: {err}"}}"#));

    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| Response::new("{\"error\":\"failed to build response\"}".to_string()))
}
