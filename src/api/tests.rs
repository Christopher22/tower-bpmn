use http::{Method, Request, StatusCode};
use http_body_util::{Empty, Full};
use tower_service::Service;

use crate::bpmn::{
    IncomingMessage, MetaData, Process, ProcessBuilder, Runtime, Token,
    messages::{Context, CorrelationKey, Entity, Participant, Role},
    storage::{InMemory, Storage},
};
use crate::{
    Api,
    executor::TokioExecutor,
    guards::{Guard, OpenApiSecurityScheme},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StartProcess;

impl Process for StartProcess {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new("start-process", "Starts and completes immediately");
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process.then("identity", |_token: &Token<S>, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MessageTarget;

impl Process for MessageTarget {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new("message-target", "Message target process");
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process.then("target", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitProcess;

impl Process for WaitProcess {
    type Input = CorrelationKey;
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new("wait-process", "Waits for correlated message");
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process
            .wait_for(IncomingMessage::new(MessageTarget, "incoming"))
            .then("double", |_token, value: i32| value * 2)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestrictedProcess;

impl Process for RestrictedProcess {
    type Input = i32;
    type Output = i32;

    const INITIAL_OWNER: Participant = Participant::Role(Role::new("admin"));

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new("restricted-process", "Requires admin context");
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process.then("identity", |_token: &Token<S>, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NobodyGuard;

impl Guard for NobodyGuard {
    fn context_from_request(&self, _request: &http::request::Parts) -> Context {
        std::iter::once(Participant::Nobody).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoleGuard;

impl Guard for RoleGuard {
    fn context_from_request(&self, _request: &http::request::Parts) -> Context {
        [
            Participant::Entity(Entity::new("user123")),
            Participant::Role(Role::new("admin")),
        ]
        .into_iter()
        .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ApiKeyGuard;

impl Guard for ApiKeyGuard {
    fn context_from_request(&self, _request: &http::request::Parts) -> Context {
        Context::for_entity(Entity::new("ApiKeyUser"))
    }

    fn openapi_security_scheme(&self) -> Option<OpenApiSecurityScheme> {
        Some(OpenApiSecurityScheme::new(
            "ApiKeyAuth",
            serde_json::json!({"type": "apiKey", "in": "header", "name": "x-api-key"}),
            Vec::new(),
        ))
    }
}

fn build_api() -> Api<TokioExecutor, InMemory> {
    let mut runtime = Runtime::default();
    runtime.register_process(StartProcess).unwrap();
    runtime.register_process(MessageTarget).unwrap();
    runtime.register_process(WaitProcess).unwrap();
    Api::new("api", runtime)
}

fn build_api_with_custom_path() -> Api<TokioExecutor, InMemory> {
    let mut runtime = Runtime::default();
    runtime.register_process(StartProcess).unwrap();
    runtime.register_process(MessageTarget).unwrap();
    runtime.register_process(WaitProcess).unwrap();

    Api::builder("api", runtime)
        .add_get_json("/health", StatusCode::OK, |_, _| {
            Ok(serde_json::json!({"status":"ok"}))
        })
        .build()
}

fn build_restricted_api<G: Guard>(guard: G) -> Api<TokioExecutor, InMemory, G> {
    let mut runtime = Runtime::default();
    runtime.register_process(RestrictedProcess).unwrap();

    Api::builder("api", runtime).with_guard(guard).build()
}

fn build_api_with_exposed_wait_process<G: Guard>(
    guard: G,
    participant: Participant,
) -> Api<TokioExecutor, InMemory, G> {
    let mut runtime = Runtime::default();
    runtime.register_process(StartProcess).unwrap();
    runtime.register_process(MessageTarget).unwrap();
    runtime.register_process(WaitProcess).unwrap();

    Api::builder("api", runtime)
        .with_guard(guard)
        .with_exposed_steps(WaitProcess, participant)
        .build()
}

fn get(path: &str) -> Request<Empty<bytes::Bytes>> {
    Request::builder()
        .method(Method::GET)
        .uri(path)
        .body(Empty::new())
        .unwrap()
}

fn post(path: &str, body: serde_json::Value) -> Request<Full<bytes::Bytes>> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/json")
        .body(Full::new(bytes::Bytes::from(body.to_string())))
        .unwrap()
}

fn post_raw(path: &str, body: &str) -> Request<Full<bytes::Bytes>> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", "application/json")
        .body(Full::new(bytes::Bytes::from(body.to_string())))
        .unwrap()
}

async fn call_json<B>(
    api: &mut impl Service<
        Request<B>,
        Response = http::Response<String>,
        Error = std::convert::Infallible,
    >,
    request: Request<B>,
) -> (StatusCode, serde_json::Value)
where
    B: http_body::Body + Send + 'static,
    B::Data: bytes::Buf + Send,
    B::Error: std::fmt::Display,
{
    let response = Service::call(api, request).await.unwrap();
    let status = response.status();
    let value = serde_json::from_str::<serde_json::Value>(response.body()).unwrap();
    (status, value)
}

fn instance_has_status(body: &serde_json::Value, instance_id: &str, status: &str) -> bool {
    body["instances"].as_array().is_some_and(|instances| {
        instances.iter().any(|instance| {
            instance["id"] == instance_id
                && match &instance["status"] {
                    serde_json::Value::String(value) => value.eq_ignore_ascii_case(status),
                    serde_json::Value::Object(value) => {
                        value.keys().any(|key| key.eq_ignore_ascii_case(status))
                    }
                    _ => false,
                }
        })
    })
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_is_available_at_root_and_entrypoint() {
    let mut api = build_api();

    let (status_root, body_root) = call_json(&mut api, get("/")).await;
    assert_eq!(status_root, StatusCode::OK);
    assert_eq!(body_root["openapi"], "3.0.3");

    let (status_entry, body_entry) = call_json(&mut api, get("/api/")).await;
    assert_eq!(status_entry, StatusCode::OK);
    assert_eq!(body_entry["info"]["title"], "tower-bpmn API");
    assert!(body_entry["paths"]["/start-process-1"].is_object());
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_lists_process_paths_and_raw_input_schemas() {
    let mut api = build_api();
    let (_, body) = call_json(&mut api, get("/api")).await;

    assert!(body["paths"]["/start-process-1"].is_object());
    assert!(body["paths"]["/start-process-1/instances"].is_object());
    assert!(body["paths"]["/wait-process-1"].is_object());

    let start_schema = &body["paths"]["/start-process-1/instances"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert_eq!(start_schema["type"], "integer");

    let wait_schema = &body["paths"]["/wait-process-1/instances"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert_eq!(wait_schema["type"], "string");
    assert_eq!(wait_schema["format"], "uuid");

    let waiting_message_schema = &body["paths"]["/wait-process-1/step/incoming/{id}"]["post"]["requestBody"]
        ["content"]["application/json"]["schema"];
    assert_eq!(waiting_message_schema["type"], "integer");

    let waiting_message_parameter =
        &body["paths"]["/wait-process-1/step/incoming/{id}"]["post"]["parameters"][0];
    assert_eq!(waiting_message_parameter["name"], "id");
    assert_eq!(waiting_message_parameter["in"], "path");
    assert_eq!(waiting_message_parameter["required"], true);
    assert_eq!(
        waiting_message_parameter["schema"]["$ref"],
        "#/components/schemas/CorrelationKey"
    );
    assert_eq!(
        body["components"]["schemas"]["CorrelationKey"]["type"],
        "string"
    );
    assert_eq!(
        body["components"]["schemas"]["CorrelationKey"]["format"],
        "uuid"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_exposes_expected_component_schemas() {
    let mut api = build_api();
    let (_, body) = call_json(&mut api, get("/")).await;

    assert!(body["components"]["schemas"]["Error"].is_object());
    assert!(body["components"]["schemas"]["ProcessSummary"].is_object());
    assert!(body["components"]["schemas"]["StartedInstanceResponse"].is_object());
    assert!(body["components"]["schemas"]["Instances"].is_object());
}

#[tokio::test(flavor = "current_thread")]
async fn process_metadata_is_available() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/api/start-process-1")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "start-process-1");
    assert_eq!(body["metadata"]["name"], "start-process");
    assert_eq!(body["metadata"]["version"], 1);
    assert_eq!(body["input_schema"]["type"], "integer");
    assert!(
        body["steps"]
            .as_array()
            .is_some_and(|steps| steps.iter().any(|step| step == "identity"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn process_instance_routes_start_instances() {
    let mut api = build_api();

    let (status, body) = call_json(
        &mut api,
        post("/api/start-process-1/instances", serde_json::json!(12)),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body["id"].is_string());
    assert_eq!(body["status"], "running");
}

#[tokio::test(flavor = "current_thread")]
async fn process_instance_routes_list_running_instances() {
    let mut api = build_api();
    let correlation_key = CorrelationKey::new();
    let (_, started) = call_json(
        &mut api,
        post(
            "/api/wait-process-1/instances",
            serde_json::to_value(correlation_key).unwrap(),
        ),
    )
    .await;
    let instance_id = started["id"].as_str().unwrap().to_string();

    let (status, body) = call_json(&mut api, get("/api/wait-process-1/instances")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(instance_has_status(&body, &instance_id, "running"));
}

#[tokio::test(flavor = "current_thread")]
async fn external_step_endpoint_sends_message_to_waiting_process() {
    let mut api = build_api();
    let correlation_key = CorrelationKey::new();

    let _ = call_json(
        &mut api,
        post(
            "/api/wait-process-1/instances",
            serde_json::to_value(correlation_key).unwrap(),
        ),
    )
    .await;

    let message_path = format!("/api/wait-process-1/step/incoming/{correlation_key}");
    let (message_status, message_body) =
        call_json(&mut api, post(&message_path, serde_json::json!(21))).await;
    assert_eq!(message_status, StatusCode::ACCEPTED);
    assert_eq!(message_body["status"], "accepted");

    let (invalid_status, invalid_body) = call_json(
        &mut api,
        post(&message_path, serde_json::json!("wrong-type")),
    )
    .await;
    assert_eq!(invalid_status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_body["message"], "invalid message type");
}

#[tokio::test(flavor = "current_thread")]
async fn returns_empty_instance_list_before_any_start() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/api/start-process-1/instances")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["instances"].as_array().unwrap().len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_invalid_json_body_syntax() {
    let mut api = build_api();
    let (status, body) = call_json(
        &mut api,
        post_raw("/api/start-process-1/instances", "{\"broken\":"),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status"], 400);
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("invalid JSON body"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_payloads_that_do_not_match_process_input_schema() {
    let mut api = build_api();
    let (status, body) = call_json(
        &mut api,
        post(
            "/api/start-process-1/instances",
            serde_json::json!("not-an-integer"),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status"], 400);
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("invalid input"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_routes_and_processes_return_not_found() {
    let mut api = build_api();

    let (status_unknown_route, body_unknown_route) =
        call_json(&mut api, get("/api/unknown-route")).await;
    assert_eq!(status_unknown_route, StatusCode::NOT_FOUND);
    assert_eq!(body_unknown_route["status"], 404);
    assert_eq!(body_unknown_route["message"], "route not found");

    let (status_unknown_process, body_unknown_process) =
        call_json(&mut api, get("/api/unknown-process-1/instances")).await;
    assert_eq!(status_unknown_process, StatusCode::NOT_FOUND);
    assert_eq!(body_unknown_process["message"], "route not found");
}

#[tokio::test(flavor = "current_thread")]
async fn unsupported_methods_return_method_not_allowed() {
    let mut api = build_api();

    let request = Request::builder()
        .method(Method::POST)
        .uri("/api/start-process-1")
        .body(Full::new(bytes::Bytes::from("1")))
        .unwrap();
    let (status_post, body_post) = call_json(&mut api, request).await;
    assert_eq!(status_post, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body_post["message"], "method not allowed");

    let request = Request::builder()
        .method(Method::PUT)
        .uri("/api/start-process-1/instances")
        .body(Full::new(bytes::Bytes::new()))
        .unwrap();
    let (status_put, body_put) = call_json(&mut api, request).await;
    assert_eq!(status_put, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body_put["status"], 405);
}

#[tokio::test(flavor = "current_thread")]
async fn trailing_slashes_are_accepted() {
    let mut api = build_api();

    let (status_instances, _) = call_json(&mut api, get("/api/start-process-1/instances/")).await;
    assert_eq!(status_instances, StatusCode::OK);
}

#[tokio::test(flavor = "current_thread")]
async fn custom_paths_can_be_registered_by_library_users() {
    let mut api = build_api_with_custom_path();

    let (status, body) = call_json(&mut api, get("/api/health")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test(flavor = "current_thread")]
async fn guard_with_nobody_cannot_access_protected_route() {
    let mut api = Api::<TokioExecutor, InMemory>::builder(
        "api",
        Runtime::<TokioExecutor, InMemory>::default(),
    )
    .with_guard(NobodyGuard)
    .add_get_json_for(
        "/health",
        Participant::Role(Role::new("admin")),
        StatusCode::OK,
        |_, _| Ok(serde_json::json!({"status":"ok"})),
    )
    .build();

    let (status, body) = call_json(&mut api, get("/api/health")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["message"], "not allowed for this participant");
}

#[tokio::test(flavor = "current_thread")]
async fn guard_context_is_used_for_process_start() {
    let mut blocked = build_restricted_api(crate::guards::EverybodyGuard);
    let (blocked_status, blocked_body) = call_json(
        &mut blocked,
        post("/api/restricted-process-1/instances", 7.into()),
    )
    .await;
    assert_eq!(blocked_status, StatusCode::FORBIDDEN);
    assert!(
        blocked_body["message"]
            .as_str()
            .is_some_and(|value| value.contains("not allowed for this participant"))
    );

    let mut allowed = build_restricted_api(RoleGuard);
    let (allowed_status, allowed_body) = call_json(
        &mut allowed,
        post("/api/restricted-process-1/instances", 7.into()),
    )
    .await;
    assert_eq!(allowed_status, StatusCode::ACCEPTED);
    assert!(allowed_body["id"].is_string());
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_includes_guard_security_scheme() {
    let mut api = Api::<TokioExecutor, InMemory>::builder(
        "api",
        Runtime::<TokioExecutor, InMemory>::default(),
    )
    .with_guard(ApiKeyGuard)
    .build();

    let (status, body) = call_json(&mut api, get("/")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["components"]["securitySchemes"]["ApiKeyAuth"].is_object());
    assert_eq!(body["security"][0]["ApiKeyAuth"], serde_json::json!([]));
}

#[tokio::test(flavor = "current_thread")]
async fn step_query_for_non_exposed_process_is_not_available() {
    let mut api =
        build_api_with_exposed_wait_process(crate::guards::EverybodyGuard, Participant::Everyone);

    let (_, started) = call_json(
        &mut api,
        post("/api/start-process-1/instances", serde_json::json!(7)),
    )
    .await;
    let instance_id = started["id"].as_str().unwrap();

    let path = format!("/api/start-process-1/step/identity/{instance_id}");
    let (status, body) = call_json(&mut api, get(&path)).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["message"], "route not found");
}

#[tokio::test(flavor = "current_thread")]
async fn step_query_requires_suitable_context() {
    let mut api = build_api_with_exposed_wait_process(
        crate::guards::EverybodyGuard,
        Participant::Role(Role::new("admin")),
    );

    let correlation_key = CorrelationKey::new();
    let (_, started) = call_json(
        &mut api,
        post(
            "/api/wait-process-1/instances",
            serde_json::to_value(correlation_key).unwrap(),
        ),
    )
    .await;
    let instance_id = started["id"].as_str().unwrap();

    let path = format!("/api/wait-process-1/step/Start/{instance_id}");
    let (status, body) = call_json(&mut api, get(&path)).await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["message"], "not allowed for this participant");
}

#[tokio::test(flavor = "current_thread")]
async fn step_query_returns_correct_history_for_external_and_normal_steps() {
    let mut api =
        build_api_with_exposed_wait_process(RoleGuard, Participant::Role(Role::new("admin")));

    let correlation_key = CorrelationKey::new();
    let (_, started) = call_json(
        &mut api,
        post(
            "/api/wait-process-1/instances",
            serde_json::to_value(correlation_key).unwrap(),
        ),
    )
    .await;
    let instance_id = started["id"].as_str().unwrap().to_string();

    let message_path = format!("/api/wait-process-1/step/incoming/{correlation_key}");
    let (message_status, _) = call_json(&mut api, post(&message_path, serde_json::json!(21))).await;
    assert_eq!(message_status, StatusCode::ACCEPTED);

    let incoming_history_path = format!("/api/wait-process-1/step/incoming/{instance_id}");
    let mut incoming_rows = Vec::new();
    for _ in 0..20 {
        let (incoming_status, incoming_body) =
            call_json(&mut api, get(&incoming_history_path)).await;
        assert_eq!(incoming_status, StatusCode::OK);
        incoming_rows = incoming_body.as_array().unwrap().clone();
        if !incoming_rows.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }

    assert_eq!(incoming_rows.len(), 1);
    assert_eq!(incoming_rows[0]["output_value"], serde_json::json!(21));
    assert!(incoming_rows[0]["timestamp"].is_string());
    assert!(incoming_rows[0]["responsible"].is_string());

    let normal_history_path = format!("/api/wait-process-1/step/double/{instance_id}");
    let mut normal_rows = Vec::new();
    for _ in 0..20 {
        let (normal_status, normal_body) = call_json(&mut api, get(&normal_history_path)).await;
        assert_eq!(normal_status, StatusCode::OK);
        normal_rows = normal_body.as_array().unwrap().clone();
        if !normal_rows.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }

    assert_eq!(normal_rows.len(), 1);
    assert_eq!(normal_rows[0]["output_value"], serde_json::json!(42));
    assert!(normal_rows[0]["timestamp"].is_string());
    assert!(normal_rows[0]["responsible"].is_string());
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_includes_exposed_step_query_paths_and_custom_output_value_schema() {
    let mut api =
        build_api_with_exposed_wait_process(crate::guards::EverybodyGuard, Participant::Everyone);

    let (_, body) = call_json(&mut api, get("/api")).await;

    assert!(body["paths"]["/wait-process-1/step/incoming/{id}"]["get"].is_object());
    assert!(body["paths"]["/wait-process-1/step/double/{id}"]["get"].is_object());
    assert!(body["paths"]["/start-process-1/step/identity/{id}"].is_null());

    let incoming_schema_ref = &body["paths"]["/wait-process-1/step/incoming/{id}"]["get"]["responses"]
        ["200"]["content"]["application/json"]["schema"]["$ref"];
    let incoming_schema_name = incoming_schema_ref
        .as_str()
        .unwrap()
        .trim_start_matches("#/components/schemas/");
    let incoming_schema = &body["components"]["schemas"][incoming_schema_name];
    let incoming_output = &incoming_schema["items"]["properties"]["output_value"];
    assert_eq!(incoming_output["type"], "integer");
    assert!(incoming_schema["items"]["properties"]["output"].is_null());

    let double_schema_ref = &body["paths"]["/wait-process-1/step/double/{id}"]["get"]["responses"]
        ["200"]["content"]["application/json"]["schema"]["$ref"];
    let double_schema_name = double_schema_ref
        .as_str()
        .unwrap()
        .trim_start_matches("#/components/schemas/");
    let double_schema = &body["components"]["schemas"][double_schema_name];
    let double_output = &double_schema["items"]["properties"]["output_value"];
    assert_eq!(double_output["type"], "integer");
}

#[tokio::test(flavor = "current_thread")]
async fn bpmn_xml_is_available_for_registered_processes() {
    let mut api = build_api();

    let request = get("/api/start-process-1/bpmn");
    let response = Service::call(&mut api, request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/xml"
    );
    let body = response.body();
    assert!(body.contains("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
    assert!(body.contains("<bpmn:process "));
}
