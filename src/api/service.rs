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

use crate::{ExtendedExecutor, ProcessName, Runtime, StorageBackend, messages};

use super::{
    error::ApiError,
    openapi,
    response::{
        AcceptedResponse, ProcessListResponse, ProcessMetadataResponse, SendMessageRequest,
        StartInstanceRequest, StartInstanceResponse, decode_json_payload, json_response,
        parse_json_body,
    },
};

/// A service that exposes the BPMN runtime API over HTTP.
#[derive(Clone)]
pub struct Api<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    Arc<(&'static str, RwLock<Runtime<E, B>>)>,
);

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Api<E, B> {
    /// Creates a new API service with the given runtime.
    pub fn new(entry_point: &'static str, runtime: Runtime<E, B>) -> Self {
        Api(Arc::new((entry_point, RwLock::new(runtime))))
    }

    async fn handle_request<Bo: Body + Send + 'static>(
        api_entry_point: &str,
        runtime: &RwLock<Runtime<E, B>>,
        request: Request<Bo>,
    ) -> Result<Response<String>, ApiError>
    where
        Bo::Data: bytes::Buf + Send,
        Bo::Error: std::fmt::Display,
    {
        let (parts, body) = request.into_parts();
        let method = parts.method;
        let segments: Vec<&str> = parts
            .uri
            .path()
            .trim_matches('/')
            .split('/')
            .take_while(|value| *value != api_entry_point)
            .collect();

        match (method.as_str(), segments.as_slice()) {
            ("GET", []) => {
                let runtime = runtime.read();
                Ok(json_response(StatusCode::OK, &openapi(&runtime)))
            }
            ("GET", ["processes"]) => {
                let runtime = runtime.read();
                Ok(json_response(
                    StatusCode::OK,
                    &ProcessListResponse {
                        processes: runtime
                            .registered_processes()
                            .map(|value| ProcessName::from(&value.meta_data))
                            .collect(),
                    },
                ))
            }
            ("GET", ["processes", process_name]) => {
                let process_name = process_name.parse::<ProcessName>()?;
                Ok(json_response(
                    StatusCode::OK,
                    &ProcessMetadataResponse {
                        process: runtime
                            .read()
                            .get_registered_process(&process_name)
                            .ok_or_else(|| ApiError::not_found("unknown process"))?
                            .meta_data
                            .clone(),
                    },
                ))
            }
            ("POST", ["processes", process_name, "instances"]) => {
                let request: StartInstanceRequest =
                    parse_json_body(body).await.and_then(decode_json_payload)?;
                let process_name = process_name.parse::<ProcessName>()?;

                Ok(json_response(
                    StatusCode::ACCEPTED,
                    &StartInstanceResponse {
                        instance_id: runtime.read().run_dynamic(&process_name, request.input)?,
                    },
                ))
            }
            ("GET", ["processes", process_name, "instances"]) => {
                let process_name = process_name.parse::<ProcessName>()?;
                Ok(json_response(
                    StatusCode::OK,
                    runtime
                        .read()
                        .get_instances(&process_name)
                        .ok_or_else(|| ApiError::not_found("unknown process"))?,
                ))
            }
            ("POST", ["processes", process_name, "messages"]) => {
                let request: SendMessageRequest =
                    parse_json_body(body).await.and_then(decode_json_payload)?;
                runtime.read().messages.send_dynamic(
                    &process_name.parse::<ProcessName>()?,
                    request.correlation_key,
                    request.payload,
                    messages::Context::default(),
                )?;

                Ok(json_response(
                    StatusCode::ACCEPTED,
                    &AcceptedResponse::default(),
                ))
            }
            _ => Err(ApiError::not_found("route not found")),
        }
    }
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
            let (api_entry_point, runtime) = &*state;
            match Self::handle_request(api_entry_point, runtime, req).await {
                Ok(response) => Ok(response),
                Err(err) => Ok(err.into_response()),
            }
        })
    }
}
