//! Generic OpenAPI renderer for route-provided metadata.

use std::collections::BTreeMap;

use serde::Serialize;

use super::{guard::OpenApiSecurityScheme, route::Route};
use crate::bpmn::{ExtendedExecutor, StorageBackend};

/// Serializable route-derived OpenAPI view used by the renderer.
#[derive(Default)]
pub(super) struct OpenApiRouteData {
    pub(super) paths: BTreeMap<String, OpenApiPathData>,
    pub(super) components: BTreeMap<String, serde_json::Value>,
}

/// One OpenAPI path item collected from one route node.
#[derive(Default)]
pub(super) struct OpenApiPathData {
    pub(super) get: Option<OpenApiOperationData>,
    pub(super) post: Option<OpenApiOperationData>,
}

/// Operation metadata that will be rendered to OpenAPI.
pub(super) struct OpenApiOperationData {
    pub(super) summary: &'static str,
    pub(super) request_body: Option<serde_json::Value>,
    pub(super) responses: BTreeMap<String, OpenApiResponseData>,
    pub(super) is_public: bool,
}

/// Response metadata that will be rendered to OpenAPI.
pub(super) struct OpenApiResponseData {
    pub(super) description: &'static str,
    pub(super) content: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct OpenApiDocument {
    openapi: &'static str,
    info: OpenApiInfo,
    paths: BTreeMap<String, PathItem>,
    components: OpenApiComponents,
    #[serde(skip_serializing_if = "Option::is_none")]
    security: Option<Vec<BTreeMap<String, Vec<String>>>>,
}

#[derive(Serialize)]
struct OpenApiInfo {
    title: &'static str,
    version: &'static str,
}

#[derive(Serialize)]
struct OpenApiComponents {
    schemas: BTreeMap<String, serde_json::Value>,
    #[serde(rename = "securitySchemes", skip_serializing_if = "Option::is_none")]
    security_schemes: Option<BTreeMap<String, serde_json::Value>>,
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
    summary: &'static str,
    responses: BTreeMap<String, OpenApiResponse>,
    #[serde(rename = "requestBody", skip_serializing_if = "Option::is_none")]
    request_body: Option<OpenApiRequestBody>,
    #[serde(skip_serializing_if = "Option::is_none")]
    security: Option<Vec<BTreeMap<String, Vec<String>>>>,
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

/// Builds the OpenAPI document from route-owned semantics.
pub(super) fn build<E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
    root: &Route<E, B>,
    security_scheme: Option<OpenApiSecurityScheme>,
) -> serde_json::Value {
    let route_data = root.openapi_data();

    let paths = route_data
        .paths
        .into_iter()
        .map(|(path, path_data)| (path, render_path(path_data)))
        .collect();

    let (security_schemes, security) = security_scheme.map_or((None, None), |value| {
        let OpenApiSecurityScheme {
            name,
            scheme,
            scopes,
        } = value;

        let mut schemes = BTreeMap::new();
        schemes.insert(name.clone(), scheme);

        let mut requirement = BTreeMap::new();
        requirement.insert(name, scopes);

        (Some(schemes), Some(vec![requirement]))
    });

    serde_json::to_value(OpenApiDocument {
        openapi: "3.0.3",
        info: OpenApiInfo {
            title: "tower-bpmn API",
            version: "1.0.0",
        },
        paths,
        components: OpenApiComponents {
            schemas: route_data.components,
            security_schemes,
        },
        security,
    })
    .expect("openapi serialization should succeed")
}

fn render_path(path_data: OpenApiPathData) -> PathItem {
    PathItem {
        get: path_data.get.map(render_operation),
        post: path_data.post.map(render_operation),
    }
}

fn render_operation(operation: OpenApiOperationData) -> Operation {
    Operation {
        summary: operation.summary,
        responses: operation
            .responses
            .into_iter()
            .map(|(status, response)| (status, render_response(response)))
            .collect(),
        request_body: operation.request_body.map(|schema| OpenApiRequestBody {
            required: true,
            content: json_content(schema),
        }),
        security: if operation.is_public {
            Some(Vec::new())
        } else {
            None
        },
    }
}

fn render_response(response: OpenApiResponseData) -> OpenApiResponse {
    OpenApiResponse {
        description: response.description,
        content: response.content.map(json_content),
    }
}

fn json_content(schema: serde_json::Value) -> OpenApiContent {
    OpenApiContent {
        application_json: OpenApiMediaType { schema },
    }
}
