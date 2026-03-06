use http::{Response, StatusCode, header::CONTENT_TYPE};
use http_body::Body;
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};

use crate::{
    CorrelationKey, ExtendedExecutor, InstanceId, InstanceStatus, Runtime, RuntimeInstance,
};

use super::error::ApiError;

#[derive(Debug, Deserialize)]
pub(super) struct StartInstanceRequest {
    pub(super) input: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub(super) struct StartInstanceResponse {
    pub(super) instance_id: InstanceId,
}

#[derive(Debug, Deserialize)]
pub(super) struct SendMessageRequest {
    pub(super) correlation_key: CorrelationKey,
    pub(super) payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub(super) struct AcceptedResponse {
    status: &'static str,
}

impl Default for AcceptedResponse {
    fn default() -> Self {
        Self { status: "accepted" }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct ProcessListResponse {
    pub(super) processes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(super) struct ErrorBody {
    pub(super) error: String,
}

pub(super) async fn parse_json_body<B>(body: B) -> Result<serde_json::Value, ApiError>
where
    B: Body,
    B::Data: bytes::Buf + Send,
    B::Error: std::fmt::Display,
{
    let bytes = body
        .collect()
        .await
        .map_err(|err| ApiError::bad_request(format!("invalid request body: {err}")))?
        .to_bytes();

    serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|err| ApiError::bad_request(format!("invalid JSON body: {err}")))
}

pub(super) fn decode_json_payload<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
) -> Result<T, ApiError> {
    serde_json::from_value(value)
        .map_err(|err| ApiError::bad_request(format!("invalid request payload: {err}")))
}

pub(super) fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Response<String> {
    let body = serde_json::to_string(value)
        .unwrap_or_else(|err| format!(r#"{{"error":"failed to serialize response: {}"}}"#, err));

    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| Response::new("{\"error\":\"failed to build response\"}".to_string()))
}

pub(super) fn process_instances_response<E: ExtendedExecutor>(
    runtime: &Runtime<E>,
    process_name: &str,
) -> Response<String> {
    let mut body = String::from("{\"instances\":[");
    let mut is_first = true;

    for entry in runtime.instances.iter() {
        let instance: &RuntimeInstance = &entry;
        if instance.process != process_name || !matches!(instance.status, InstanceStatus::Running) {
            continue;
        }

        if !is_first {
            body.push(',');
        }
        is_first = false;

        match serde_json::to_string(instance) {
            Ok(instance_json) => body.push_str(&instance_json),
            Err(err) => {
                return json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &ErrorBody {
                        error: format!("failed to serialize instance response: {err}"),
                    },
                );
            }
        }
    }

    body.push_str("]}");

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| Response::new("{\"error\":\"failed to build response\"}".to_string()))
}

pub fn openapi() -> serde_json::Value {
    serde_json::json!({
        "openapi": "3.1.0",
        "info": {
            "title": "axum-bpmn API",
            "version": "1.0.0"
        },
        "paths": {
            "/": {
                "get": {
                    "summary": "OpenAPI definition",
                    "responses": {
                        "200": {
                            "description": "OpenAPI document"
                        }
                    }
                }
            },
            "/processes": {
                "get": {
                    "summary": "List process definitions",
                    "responses": {
                        "200": { "description": "Registered process names" }
                    }
                }
            },
            "/processes/{process}/instances": {
                "post": {
                    "summary": "Start a process instance",
                    "responses": {
                        "202": { "description": "Instance started" }
                    }
                },
                "get": {
                    "summary": "List running instances for a process",
                    "responses": {
                        "200": { "description": "Running instances" }
                    }
                }
            },
            "/instances/{instance_id}": {
                "get": {
                    "summary": "Get instance state",
                    "responses": {
                        "200": { "description": "Instance state" },
                        "404": { "description": "Instance not found" }
                    }
                }
            },
            "/processes/{process}/messages": {
                "post": {
                    "summary": "Send correlated message to waiting instance(s)",
                    "responses": {
                        "202": { "description": "Message accepted" }
                    }
                }
            }
        }
    })
}
