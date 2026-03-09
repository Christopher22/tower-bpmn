use http::{Response, StatusCode, header::CONTENT_TYPE};
use http_body::Body;
use http_body_util::BodyExt;
use schemars::{JsonSchema, json_schema};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    CorrelationKey, ExtendedExecutor, Instance, InstanceId, MetaData, Runtime, StorageBackend,
};

use super::error::ApiError;

#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct StartInstanceRequest {
    pub(super) input: serde_json::Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub(super) struct StartInstanceResponse {
    pub(super) instance_id: InstanceId,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct SendMessageRequest {
    pub(super) correlation_key: CorrelationKey,
    pub(super) payload: serde_json::Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub(super) struct AcceptedResponse {
    status: &'static str,
}

impl Default for AcceptedResponse {
    fn default() -> Self {
        Self { status: "accepted" }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
pub(super) struct ProcessListResponse {
    pub(super) processes: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub(super) struct ProcessMetadataResponse {
    pub(super) process: MetaData,
}

#[derive(Debug, Serialize, JsonSchema)]
pub(super) struct InstancePlacesResponse {
    pub(super) instance_id: InstanceId,
    pub(super) places: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
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
        .unwrap_or_else(|err| format!(r#"{{"error":"failed to serialize response: {err}"}}"#));

    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap_or_else(|_| Response::new("{\"error\":\"failed to build response\"}".to_string()))
}

fn schema_for<T: JsonSchema>() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("failed to serialize schema")
}

#[derive(Serialize)]
struct OpenApiDocument {
    openapi: &'static str,
    info: OpenApiInfo,
    paths: BTreeMap<String, PathItem>,
    components: OpenApiComponents,
}

#[derive(Serialize)]
struct OpenApiInfo {
    title: &'static str,
    version: &'static str,
}

#[derive(Serialize)]
struct OpenApiComponents {
    schemas: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize, Default)]
struct PathItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    get: Option<Operation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post: Option<Operation>,
}

#[derive(Serialize)]
struct Operation {
    summary: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    parameters: Vec<OpenApiParameter>,
    #[serde(rename = "requestBody", skip_serializing_if = "Option::is_none")]
    request_body: Option<OpenApiRequestBody>,
    responses: BTreeMap<String, OpenApiResponse>,
}

#[derive(Serialize)]
struct OpenApiParameter {
    name: &'static str,
    #[serde(rename = "in")]
    in_location: &'static str,
    required: bool,
    schema: serde_json::Value,
}

#[derive(Serialize)]
struct OpenApiRequestBody {
    required: bool,
    content: OpenApiContent,
}

#[derive(Serialize)]
struct OpenApiResponse {
    description: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<OpenApiContent>,
}

#[derive(Serialize)]
struct OpenApiContent {
    #[serde(rename = "application/json")]
    application_json: OpenApiMediaType,
}

#[derive(Serialize)]
struct OpenApiMediaType {
    schema: serde_json::Value,
}

fn schema_ref(schema_name: &str) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    object.insert(
        "$ref".to_string(),
        serde_json::Value::String(format!("#/components/schemas/{schema_name}")),
    );
    serde_json::Value::Object(object)
}

fn object_schema(
    properties: impl IntoIterator<Item = (String, serde_json::Value)>,
    required: impl IntoIterator<Item = String>,
) -> serde_json::Value {
    let properties_map = properties
        .into_iter()
        .collect::<serde_json::Map<String, serde_json::Value>>();
    let required_values = required
        .into_iter()
        .map(serde_json::Value::String)
        .collect::<Vec<_>>();

    let mut object = serde_json::Map::new();
    object.insert(
        "type".to_string(),
        serde_json::Value::String("object".to_string()),
    );
    object.insert(
        "properties".to_string(),
        serde_json::Value::Object(properties_map),
    );
    object.insert(
        "required".to_string(),
        serde_json::Value::Array(required_values),
    );
    object.insert(
        "additionalProperties".to_string(),
        serde_json::Value::Bool(false),
    );

    serde_json::Value::Object(object)
}

fn json_content(schema: serde_json::Value) -> OpenApiContent {
    OpenApiContent {
        application_json: OpenApiMediaType { schema },
    }
}

fn schema_response(description: &'static str, schema_name: &str) -> OpenApiResponse {
    OpenApiResponse {
        description,
        content: Some(json_content(schema_ref(schema_name))),
    }
}

fn openapi_operation(summary: impl Into<String>) -> Operation {
    Operation {
        summary: summary.into(),
        parameters: Vec::new(),
        request_body: None,
        responses: BTreeMap::new(),
    }
}

pub fn openapi<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    runtime: &Runtime<E, B>,
) -> serde_json::Value {
    let mut paths = BTreeMap::new();

    let mut root_get = openapi_operation("OpenAPI definition");
    root_get.responses.insert(
        "200".to_string(),
        OpenApiResponse {
            description: "OpenAPI document",
            content: Some(json_content(object_schema(Vec::new(), Vec::new()))),
        },
    );
    paths.insert(
        "/".to_string(),
        PathItem {
            get: Some(root_get),
            post: None,
        },
    );

    let mut processes_get = openapi_operation("List process definitions");
    processes_get.responses.insert(
        "200".to_string(),
        schema_response("Registered process names", "ProcessListResponse"),
    );
    paths.insert(
        "/processes".to_string(),
        PathItem {
            get: Some(processes_get),
            post: None,
        },
    );

    let mut instance_get = openapi_operation("Get instance state");
    instance_get.parameters.push(OpenApiParameter {
        name: "instance_id",
        in_location: "path",
        required: true,
        schema: schema_for::<InstanceId>(),
    });
    instance_get.responses.insert(
        "200".to_string(),
        schema_response("Instance state", "RuntimeInstance"),
    );
    instance_get.responses.insert(
        "404".to_string(),
        schema_response("Instance not found", "ErrorBody"),
    );
    paths.insert(
        "/instances/{instance_id}".to_string(),
        PathItem {
            get: Some(instance_get),
            post: None,
        },
    );

    let mut instance_places_get = openapi_operation("Get current places for an instance");
    instance_places_get.parameters.push(OpenApiParameter {
        name: "instance_id",
        in_location: "path",
        required: true,
        schema: schema_for::<InstanceId>(),
    });
    instance_places_get.responses.insert(
        "200".to_string(),
        schema_response("Current instance places", "InstancePlacesResponse"),
    );
    instance_places_get.responses.insert(
        "400".to_string(),
        schema_response("Invalid instance id", "ErrorBody"),
    );
    instance_places_get.responses.insert(
        "404".to_string(),
        schema_response("Instance not found", "ErrorBody"),
    );
    paths.insert(
        "/instances/{instance_id}/places".to_string(),
        PathItem {
            get: Some(instance_places_get),
            post: None,
        },
    );

    for process in runtime.registered_processes() {
        let mut process_get = openapi_operation("Get process metadata");
        process_get.responses.insert(
            "200".to_string(),
            schema_response("Process metadata", "ProcessMetadataResponse"),
        );
        process_get.responses.insert(
            "404".to_string(),
            schema_response("Process not found", "ErrorBody"),
        );
        paths.insert(
            format!("/processes/{process}/"),
            PathItem {
                get: Some(process_get),
                post: None,
            },
        );

        let instances_path = format!("/processes/{process}/instances");
        let mut instances_post = openapi_operation("Start a process instance");
        instances_post.request_body = Some(OpenApiRequestBody {
            required: true,
            content: json_content(object_schema(
                [("input".to_string(), process.input_schema.clone())],
                ["input".to_string()],
            )),
        });
        instances_post.responses.insert(
            "202".to_string(),
            schema_response("Instance started", "StartInstanceResponse"),
        );
        instances_post.responses.insert(
            "400".to_string(),
            schema_response("Invalid payload", "ErrorBody"),
        );
        instances_post.responses.insert(
            "404".to_string(),
            schema_response("Process not found", "ErrorBody"),
        );

        let mut instances_get = openapi_operation("List running instances for a process");
        instances_get.responses.insert(
            "200".to_string(),
            schema_response("Running instances", "ProcessInstancesResponse"),
        );
        instances_get.responses.insert(
            "404".to_string(),
            schema_response("Process not found", "ErrorBody"),
        );

        paths.insert(
            instances_path,
            PathItem {
                get: Some(instances_get),
                post: Some(instances_post),
            },
        );

        let messages_path = format!("/processes/{process}/messages");
        let mut messages_post = openapi_operation("Send correlated message to waiting instance(s)");
        messages_post.request_body = Some(OpenApiRequestBody {
            required: true,
            content: json_content(object_schema(
                [
                    (
                        "correlation_key".to_string(),
                        schema_for::<CorrelationKey>(),
                    ),
                    ("payload".to_string(), process.input_schema.clone()),
                ],
                ["correlation_key".to_string(), "payload".to_string()],
            )),
        });
        messages_post.responses.insert(
            "202".to_string(),
            schema_response("Message accepted", "AcceptedResponse"),
        );
        messages_post.responses.insert(
            "400".to_string(),
            schema_response("Invalid payload", "ErrorBody"),
        );
        messages_post.responses.insert(
            "404".to_string(),
            schema_response("Process not found", "ErrorBody"),
        );

        paths.insert(
            messages_path,
            PathItem {
                get: None,
                post: Some(messages_post),
            },
        );
    }

    let components = OpenApiComponents {
        schemas: [
            (
                "StartInstanceRequest".to_string(),
                schema_for::<StartInstanceRequest>(),
            ),
            (
                "StartInstanceResponse".to_string(),
                schema_for::<StartInstanceResponse>(),
            ),
            (
                "SendMessageRequest".to_string(),
                schema_for::<SendMessageRequest>(),
            ),
            (
                "AcceptedResponse".to_string(),
                schema_for::<AcceptedResponse>(),
            ),
            (
                "ProcessListResponse".to_string(),
                schema_for::<ProcessListResponse>(),
            ),
            (
                "ProcessMetadataResponse".to_string(),
                schema_for::<ProcessMetadataResponse>(),
            ),
            (
                "ProcessInstancesResponse".to_string(),
                json_schema!({
                    "type": "object",
                    "properties": {
                        "instances": {
                            "type": "array",
                            "items": schema_ref("RuntimeInstance"),
                        },
                    },
                    "required": ["instances"],
                    "additionalProperties": false,
                })
                .into(),
            ),
            (
                "InstancePlacesResponse".to_string(),
                schema_for::<InstancePlacesResponse>(),
            ),
            (
                "RuntimeInstance".to_string(),
                schema_for::<Instance<E, B>>(),
            ),
            ("ErrorBody".to_string(), schema_for::<ErrorBody>()),
        ]
        .into_iter()
        .collect(),
    };

    let doc = OpenApiDocument {
        openapi: "3.1.0",
        info: OpenApiInfo {
            title: "axum-bpmn API",
            version: "1.0.0",
        },
        paths,
        components,
    };

    serde_json::to_value(doc).expect("failed to serialize openapi document")
}
