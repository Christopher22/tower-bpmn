use std::{
    convert::Infallible,
    future::Future,
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use http::{Request, Response, StatusCode};
use http_body::Body;
use parking_lot::RwLock;
use tower_service::Service;

use crate::{ExtendedExecutor, InstanceId, Runtime, StorageBackend};

use super::{
    error::ApiError,
    openapi,
    response::{
        AcceptedResponse, InstancePlacesResponse, ProcessListResponse, ProcessMetadataResponse,
        SendMessageRequest, StartInstanceRequest, StartInstanceResponse, decode_json_payload,
        json_response, parse_json_body,
    },
};

/// A service that exposes the BPMN runtime API over HTTP.
#[derive(Clone)]
pub struct Api<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    Arc<(&'static str, RwLock<Runtime<E, B>>)>,
);

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> std::fmt::Debug for Api<E, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api").finish_non_exhaustive()
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Api<E, B> {
    /// Creates a new API service with the given runtime.
    pub fn new(entry_point: &'static str, runtime: Runtime<E, B>) -> Self {
        Api(Arc::new((entry_point, RwLock::new(runtime))))
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
            let (parts, body) = req.into_parts();
            let method = parts.method;
            let segments: Vec<&str> = parts
                .uri
                .path()
                .trim_matches('/')
                .split('/')
                .take_while(|value| value != api_entry_point)
                .collect();

            let response = match (method.as_str(), segments.as_slice()) {
                ("GET", []) => {
                    let runtime = runtime.read();
                    json_response(StatusCode::OK, &openapi(&runtime))
                }
                ("GET", ["processes"]) => {
                    let runtime = runtime.read();
                    json_response(
                        StatusCode::OK,
                        &ProcessListResponse {
                            processes: runtime
                                .registered_processes()
                                .map(|value| value.meta_data.name.to_string())
                                .collect(),
                        },
                    )
                }
                ("GET", ["processes", process_name]) => {
                    let runtime = runtime.read();
                    match runtime
                        .registered_processes()
                        .find(|value| value.meta_data.name == *process_name)
                        .map(|process| process.meta_data.clone())
                    {
                        Some(process) => {
                            json_response(StatusCode::OK, &ProcessMetadataResponse { process })
                        }
                        None => ApiError::not_found("unknown process").into_response(),
                    }
                }
                ("POST", ["processes", process_name, "instances"]) => {
                    let request: StartInstanceRequest =
                        match parse_json_body(body).await.and_then(decode_json_payload) {
                            Ok(request) => request,
                            Err(err) => return Ok(err.into_response()),
                        };

                    let runtime = runtime.read();
                    match runtime.run_dynamic(process_name, request.input) {
                        Ok(instance_id) => json_response(
                            StatusCode::ACCEPTED,
                            &StartInstanceResponse { instance_id },
                        ),
                        Err(err) => ApiError::from_runtime_api_error(err).into_response(),
                    }
                }
                ("GET", ["processes", process_name, "instances"]) => {
                    let runtime = runtime.read();
                    match runtime
                        .instances()
                        .find(|p| p.registered_process.meta_data.name == *process_name)
                    {
                        Some(instances) => json_response(
                            StatusCode::OK,
                            &serde_json::json!({
                                "instances": instances
                                            .iter()
                                            .map(|instance| {
                                                serde_json::to_value(instance.deref())
                                                    .expect("unable to serialize instance")
                                            })
                                            .collect::<Vec<_>>(),
                            }),
                        ),
                        None => ApiError::not_found("unknown process").into_response(),
                    }
                }
                ("GET", ["instances", instance_id, "places"]) => {
                    let instance_id = match instance_id.parse::<InstanceId>() {
                        Ok(instance_id) => instance_id,
                        Err(err) => {
                            return Ok(ApiError::bad_request(format!(
                                "invalid instance id: {err}"
                            ))
                            .into_response());
                        }
                    };

                    let runtime = runtime.read();
                    match runtime.instance_places(instance_id) {
                        Some(places) => json_response(
                            StatusCode::OK,
                            &InstancePlacesResponse {
                                instance_id,
                                places,
                            },
                        ),
                        None => ApiError::not_found("unknown instance").into_response(),
                    }
                }
                ("POST", ["processes", process_name, "messages"]) => {
                    let request: SendMessageRequest =
                        match parse_json_body(body).await.and_then(decode_json_payload) {
                            Ok(request) => request,
                            Err(err) => return Ok(err.into_response()),
                        };

                    let runtime = runtime.read();
                    match runtime.send_message_dynamic(
                        process_name,
                        request.correlation_key,
                        request.payload,
                    ) {
                        Ok(()) => json_response(StatusCode::ACCEPTED, &AcceptedResponse::default()),
                        Err(err) => ApiError::from_runtime_api_error(err).into_response(),
                    }
                }
                _ => ApiError::not_found("route not found").into_response(),
            };

            Ok(response)
        })
    }
}
