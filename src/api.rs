use std::{
    collections::{BTreeMap, HashMap},
    convert::Infallible,
    sync::Arc,
    task::{Context, Poll},
};

use http::{Method, Request, Response, StatusCode};
use schemars::schema_for;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower::Service;

use crate::{CorrelationKey, ExtendedExecutor, Message, Process, Runtime, SendError};

type StartHandler<E> =
    Arc<dyn Fn(&Runtime<E>, serde_json::Value) -> Result<uuid::Uuid, ApiError> + Send + Sync>;
type MessageHandler<E> = Arc<
    dyn Fn(&Runtime<E>, CorrelationKey, serde_json::Value) -> Result<(), ApiError> + Send + Sync,
>;

#[derive(Debug, Clone)]
struct ValueSchemaEntry {
    schema_name: String,
    schema: serde_json::Value,
}

/// Builder for a tower-native runtime API service.
#[derive(Clone)]
pub struct EngineApiServiceBuilder<E: ExtendedExecutor> {
    runtime: Arc<Runtime<E>>,
    start_handlers: HashMap<String, StartHandler<E>>,
    message_handlers: HashMap<String, MessageHandler<E>>,
    start_value_schemas: Vec<ValueSchemaEntry>,
    message_value_schemas: Vec<ValueSchemaEntry>,
}

impl<E: ExtendedExecutor> std::fmt::Debug for EngineApiServiceBuilder<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineApiServiceBuilder")
            .field("start_handlers", &self.start_handlers.len())
            .field("message_handlers", &self.message_handlers.len())
            .field("start_value_schemas", &self.start_value_schemas.len())
            .field("message_value_schemas", &self.message_value_schemas.len())
            .finish_non_exhaustive()
    }
}

impl<E: ExtendedExecutor + Send + Sync + 'static> EngineApiServiceBuilder<E> {
    /// Create a builder from an existing runtime with registered processes.
    pub fn new(runtime: Arc<Runtime<E>>) -> Self {
        Self {
            runtime,
            start_handlers: HashMap::new(),
            message_handlers: HashMap::new(),
            start_value_schemas: Vec::new(),
            message_value_schemas: Vec::new(),
        }
    }

    /// Expose a typed start endpoint for a process and include its input schema in OpenAPI.
    pub fn expose_start<P>(mut self, process: P) -> Self
    where
        P: Process + Clone + Send + Sync + 'static,
    {
        let process_name = process.name().to_string();
        self.start_handlers.insert(
            process_name,
            Arc::new(move |runtime, payload| {
                let typed_payload: P::Input = serde_json::from_value(payload)
                    .map_err(|error| ApiError::BadRequest(format!("invalid payload: {error}")))?;
                let instance = runtime
                    .run(process.clone(), typed_payload)
                    .map_err(ApiError::from)?;
                Ok(instance.id())
            }),
        );
        self.start_value_schemas
            .push(schema_entry_for::<P::Input>());
        self
    }

    /// Expose a typed message endpoint for a process and include its payload schema in OpenAPI.
    pub fn expose_message<P, V>(mut self, process: P) -> Self
    where
        P: Process + Clone + Send + Sync + 'static,
        V: crate::Value,
    {
        let process_name = process.name().to_string();
        self.message_handlers.insert(
            process_name,
            Arc::new(move |runtime, correlation_key, payload| {
                let typed_payload: V = serde_json::from_value(payload)
                    .map_err(|error| ApiError::BadRequest(format!("invalid payload: {error}")))?;
                runtime
                    .send_message(Message {
                        process: process.clone(),
                        payload: typed_payload,
                        correlation_key,
                    })
                    .map_err(ApiError::from)
            }),
        );
        self.message_value_schemas.push(schema_entry_for::<V>());
        self
    }

    /// Build the tower service.
    pub fn build(self) -> EngineApiService<E> {
        let openapi = build_openapi_document(
            &self.runtime,
            &self.start_value_schemas,
            &self.message_value_schemas,
        );
        EngineApiService {
            state: Arc::new(ApiState {
                runtime: self.runtime,
                start_handlers: self.start_handlers,
                message_handlers: self.message_handlers,
                openapi,
            }),
        }
    }
}

#[derive(Clone)]
struct ApiState<E: ExtendedExecutor> {
    runtime: Arc<Runtime<E>>,
    start_handlers: HashMap<String, StartHandler<E>>,
    message_handlers: HashMap<String, MessageHandler<E>>,
    openapi: serde_json::Value,
}

/// HTTP API service implemented as a tower `Service`.
#[derive(Clone)]
pub struct EngineApiService<E: ExtendedExecutor> {
    state: Arc<ApiState<E>>,
}

impl<E: ExtendedExecutor> std::fmt::Debug for EngineApiService<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineApiService").finish_non_exhaustive()
    }
}

impl<E: ExtendedExecutor + Send + Sync + 'static> Service<Request<Vec<u8>>>
    for EngineApiService<E>
{
    type Response = Response<Vec<u8>>;
    type Error = Infallible;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Vec<u8>>) -> Self::Future {
        let state = self.state.clone();
        Box::pin(async move { Ok(handle_request(state, req)) })
    }
}

fn handle_request<E: ExtendedExecutor + Send + Sync + 'static>(
    state: Arc<ApiState<E>>,
    req: Request<Vec<u8>>,
) -> Response<Vec<u8>> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let body = req.into_body();

    match (method, path.as_str()) {
        (Method::GET, "/processes") => json_response(StatusCode::OK, &list_processes(&state)),
        (Method::GET, "/instances/running") => {
            json_response(StatusCode::OK, &state.runtime.running_instances())
        }
        (Method::GET, "/openapi.json") => json_response(StatusCode::OK, &state.openapi),
        (Method::POST, _) if path.starts_with("/processes/") && path.ends_with("/start") => {
            match extract_process_name(&path, "/start") {
                Some(process) => match decode_json::<StartProcessRequest>(&body)
                    .and_then(|request| start_process(&state, process, request))
                {
                    Ok(response) => json_response(StatusCode::OK, &response),
                    Err(error) => error_response(error),
                },
                None => not_found_response(),
            }
        }
        (Method::POST, _) if path.starts_with("/processes/") && path.ends_with("/messages") => {
            match extract_process_name(&path, "/messages") {
                Some(process) => match decode_json::<SendMessageRequest>(&body)
                    .and_then(|request| send_message(&state, process, request))
                {
                    Ok(()) => empty_response(StatusCode::ACCEPTED),
                    Err(error) => error_response(error),
                },
                None => not_found_response(),
            }
        }
        _ => not_found_response(),
    }
}

fn extract_process_name<'a>(path: &'a str, suffix: &str) -> Option<&'a str> {
    let prefix = "/processes/";
    path.strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .and_then(|value| if value.is_empty() { None } else { Some(value) })
}

fn decode_json<T: for<'a> Deserialize<'a>>(payload: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(payload)
        .map_err(|error| ApiError::BadRequest(format!("invalid JSON body: {error}")))
}

fn start_process<E: ExtendedExecutor + Send + Sync + 'static>(
    state: &ApiState<E>,
    process: &str,
    request: StartProcessRequest,
) -> Result<StartedInstance, ApiError> {
    let start = state.start_handlers.get(process).ok_or_else(|| {
        ApiError::NotFound(format!(
            "process '{process}' is not startable through this API"
        ))
    })?;

    let instance_id = start(&state.runtime, request.payload)?;
    Ok(StartedInstance { instance_id })
}

fn send_message<E: ExtendedExecutor + Send + Sync + 'static>(
    state: &ApiState<E>,
    process: &str,
    request: SendMessageRequest,
) -> Result<(), ApiError> {
    let send = state.message_handlers.get(process).ok_or_else(|| {
        ApiError::NotFound(format!(
            "process '{process}' does not accept messages through this API"
        ))
    })?;

    send(&state.runtime, request.correlation_key, request.payload)
}

fn list_processes<E: ExtendedExecutor + Send + Sync + 'static>(
    state: &ApiState<E>,
) -> Vec<ProcessListItem> {
    let mut processes = state.runtime.registered_processes();
    processes.sort_unstable();
    processes
        .into_iter()
        .map(|name| ProcessListItem {
            can_start: state.start_handlers.contains_key(&name),
            can_receive_messages: state.message_handlers.contains_key(&name),
            name,
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct StartProcessRequest {
    payload: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct SendMessageRequest {
    correlation_key: CorrelationKey,
    payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ProcessListItem {
    name: String,
    can_start: bool,
    can_receive_messages: bool,
}

#[derive(Debug, Serialize)]
struct StartedInstance {
    instance_id: uuid::Uuid,
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    Runtime(String),
}

impl From<crate::InstanceError> for ApiError {
    fn from(error: crate::InstanceError) -> Self {
        match error {
            crate::InstanceError::Unregistered => {
                ApiError::NotFound("process is not registered".to_string())
            }
            crate::InstanceError::Completed => {
                ApiError::Runtime("instance already completed".to_string())
            }
            crate::InstanceError::InvalidContext => {
                ApiError::BadRequest("invalid process context".to_string())
            }
        }
    }
}

impl From<SendError> for ApiError {
    fn from(error: SendError) -> Self {
        match error {
            SendError::InvalidType => {
                ApiError::BadRequest("message payload type is invalid".to_string())
            }
            SendError::NoTarget => {
                ApiError::NotFound("no message target is currently registered".to_string())
            }
        }
    }
}

fn error_response(error: ApiError) -> Response<Vec<u8>> {
    let (status, message) = match error {
        ApiError::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
        ApiError::NotFound(message) => (StatusCode::NOT_FOUND, message),
        ApiError::Runtime(message) => (StatusCode::INTERNAL_SERVER_ERROR, message),
    };
    json_response(status, &json!({ "error": message }))
}

fn json_response(status: StatusCode, value: &impl Serialize) -> Response<Vec<u8>> {
    let body = serde_json::to_vec(value).expect("response serialization should not fail");
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(body)
        .expect("response should build")
}

fn empty_response(status: StatusCode) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .body(Vec::new())
        .expect("response should build")
}

fn not_found_response() -> Response<Vec<u8>> {
    json_response(
        StatusCode::NOT_FOUND,
        &json!({ "error": "endpoint not found" }),
    )
}

fn schema_entry_for<T: crate::Value>() -> ValueSchemaEntry {
    let schema = schema_for!(T);
    ValueSchemaEntry {
        schema_name: short_type_name(std::any::type_name::<T>()),
        schema: serde_json::to_value(schema).expect("schema serialization failed"),
    }
}

fn short_type_name(full_name: &str) -> String {
    full_name
        .rsplit("::")
        .next()
        .unwrap_or(full_name)
        .to_string()
}

fn build_openapi_document<E: ExtendedExecutor + 'static>(
    runtime: &Runtime<E>,
    start_value_schemas: &[ValueSchemaEntry],
    message_value_schemas: &[ValueSchemaEntry],
) -> serde_json::Value {
    let process_names = runtime.registered_processes();

    let mut components = BTreeMap::new();
    for schema in start_value_schemas
        .iter()
        .chain(message_value_schemas.iter())
    {
        components.insert(schema.schema_name.clone(), schema.schema.clone());
    }

    let start_refs: Vec<_> = start_value_schemas
        .iter()
        .map(|schema| json!({ "$ref": format!("#/components/schemas/{}", schema.schema_name) }))
        .collect();
    let message_refs: Vec<_> = message_value_schemas
        .iter()
        .map(|schema| json!({ "$ref": format!("#/components/schemas/{}", schema.schema_name) }))
        .collect();

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "axum-bpmn runtime API",
            "version": env!("CARGO_PKG_VERSION")
        },
        "paths": {
            "/processes": {
                "get": {
                    "summary": "List registered processes",
                    "responses": {
                        "200": {
                            "description": "Registered processes"
                        }
                    }
                }
            },
            "/instances/running": {
                "get": {
                    "summary": "List running instances and current state",
                    "responses": {
                        "200": {
                            "description": "Running process instances"
                        }
                    }
                }
            },
            "/processes/{process}/start": {
                "post": {
                    "summary": "Start a process instance",
                    "parameters": [
                        {
                            "name": "process",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string",
                                "enum": process_names
                            }
                        }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["payload"],
                                    "properties": {
                                        "payload": {
                                            "oneOf": start_refs
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Started instance id"
                        }
                    }
                }
            },
            "/processes/{process}/messages": {
                "post": {
                    "summary": "Send a correlated process message",
                    "parameters": [
                        {
                            "name": "process",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string",
                                "enum": process_names
                            }
                        }
                    ],
                    "requestBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["correlation_key", "payload"],
                                    "properties": {
                                        "correlation_key": {
                                            "type": "string",
                                            "format": "uuid"
                                        },
                                        "payload": {
                                            "oneOf": message_refs
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "responses": {
                        "202": {
                            "description": "Message accepted"
                        }
                    }
                }
            },
            "/openapi.json": {
                "get": {
                    "summary": "Return OpenAPI definition",
                    "responses": {
                        "200": {
                            "description": "OpenAPI document"
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": components
        }
    })
}
