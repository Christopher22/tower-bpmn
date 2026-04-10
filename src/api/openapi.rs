//! OpenAPI document generation for the HTTP API.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Serialize;

use super::{
    error::Error,
    route::{ProcessSummary, StartedInstanceResponse},
};
use crate::{ExtendedExecutor, StorageBackend, bpmn::Runtime};

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

#[derive(Clone, Serialize, Default)]
struct PathItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    get: Option<Operation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post: Option<Operation>,
}

#[derive(Clone, Serialize)]
struct Operation {
    summary: &'static str,
    responses: BTreeMap<String, OpenApiResponse>,
    #[serde(rename = "requestBody", skip_serializing_if = "Option::is_none")]
    request_body: Option<OpenApiRequestBody>,
}

#[derive(Clone, Serialize)]
struct OpenApiRequestBody {
    required: bool,
    content: OpenApiContent,
}

#[derive(Clone, Serialize)]
struct OpenApiResponse {
    description: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<OpenApiContent>,
}

#[derive(Clone, Serialize)]
struct OpenApiContent {
    #[serde(rename = "application/json")]
    application_json: OpenApiMediaType,
}

#[derive(Clone, Serialize)]
struct OpenApiMediaType {
    schema: serde_json::Value,
}

/// Response body for the process collection endpoint.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(super) struct ProcessesResponse {
    pub(super) processes: Vec<ProcessSummary>,
}

/// Builds the OpenAPI document for the currently registered processes.
pub(super) fn build<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    runtime: &Runtime<E, B>,
) -> serde_json::Value {
    let mut paths = BTreeMap::from([
        (
            "/".to_string(),
            get_only(
                "Get OpenAPI document",
                [(
                    "200",
                    response("OpenAPI document", schema_for::<serde_json::Value>()),
                )],
            ),
        ),
        (
            "/processes".to_string(),
            PathItem {
                get: Some(operation(
                    "List registered processes",
                    None,
                    [(
                        "200",
                        response_with_ref("Registered processes", "ProcessesResponse"),
                    )],
                )),
                post: Some(operation(
                    "Method not allowed",
                    None,
                    [("405", response_with_ref("Method not allowed", "Error"))],
                )),
            },
        ),
    ]);

    for process in runtime.registered_processes() {
        let process_name = crate::ProcessName::from(&process.meta_data).to_string();
        let request_body = OpenApiRequestBody {
            required: true,
            content: json_content(process.input.json_schema.clone()),
        };
        let process_item = process_instances_path_item(request_body);

        paths.insert(format!("/processes/{process_name}"), process_item.clone());
        paths.insert(format!("/processes/{process_name}/instances"), process_item);
    }

    serde_json::to_value(OpenApiDocument {
        openapi: "3.1.0",
        info: OpenApiInfo {
            title: "axum-bpmn API",
            version: "1.0.0",
        },
        paths,
        components: OpenApiComponents {
            schemas: [
                ("Error".to_string(), schema_for::<Error>()),
                (
                    "ProcessesResponse".to_string(),
                    schema_for::<ProcessesResponse>(),
                ),
                ("ProcessSummary".to_string(), schema_for::<ProcessSummary>()),
                (
                    "StartedInstanceResponse".to_string(),
                    schema_for::<StartedInstanceResponse>(),
                ),
                (
                    "Instances".to_string(),
                    schema_for::<crate::Instances<E, B>>(),
                ),
                (
                    "Instance".to_string(),
                    schema_for::<crate::Instance<E, B>>(),
                ),
            ]
            .into_iter()
            .collect(),
        },
    })
    .expect("openapi serialization should succeed")
}

fn process_instances_path_item(request_body: OpenApiRequestBody) -> PathItem {
    PathItem {
        get: Some(operation(
            "List process instances",
            None,
            [
                ("200", response_with_ref("Process instances", "Instances")),
                ("404", response_with_ref("Unknown process", "Error")),
            ],
        )),
        post: Some(operation(
            "Start process instance",
            Some(request_body),
            [
                (
                    "202",
                    response_with_ref("Started instance", "StartedInstanceResponse"),
                ),
                ("400", response_with_ref("Invalid request", "Error")),
                ("404", response_with_ref("Unknown process", "Error")),
            ],
        )),
    }
}

fn get_only(
    summary: &'static str,
    responses: impl IntoIterator<Item = (&'static str, OpenApiResponse)>,
) -> PathItem {
    PathItem {
        get: Some(operation(summary, None, responses)),
        post: None,
    }
}

fn operation(
    summary: &'static str,
    request_body: Option<OpenApiRequestBody>,
    responses: impl IntoIterator<Item = (&'static str, OpenApiResponse)>,
) -> Operation {
    Operation {
        summary,
        responses: responses
            .into_iter()
            .map(|(status, response)| (status.to_string(), response))
            .collect(),
        request_body,
    }
}

fn response(description: &'static str, schema: serde_json::Value) -> OpenApiResponse {
    OpenApiResponse {
        description,
        content: Some(json_content(schema)),
    }
}

fn json_content(schema: serde_json::Value) -> OpenApiContent {
    OpenApiContent {
        application_json: OpenApiMediaType { schema },
    }
}

fn schema_ref(schema_name: &str) -> serde_json::Value {
    serde_json::json!({ "$ref": format!("#/components/schemas/{schema_name}") })
}

fn response_with_ref(description: &'static str, schema_name: &str) -> OpenApiResponse {
    response(description, schema_ref(schema_name))
}

fn schema_for<T: JsonSchema>() -> serde_json::Value {
    schemars::schema_for!(T).into()
}
