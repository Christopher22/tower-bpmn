use http::{Method, Request, StatusCode};
use http_body_util::{Empty, Full};
use tower_service::Service;

use crate::{
    Api, Guard, InMemory, IncomingMessage, MetaData, OpenApiSecurityScheme, Process,
    ProcessBuilder, Runtime, Storage, Token, executor::TokioExecutor,
    messages::{CorrelationKey, Participant},
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

    const INITIAL_OWNER: Participant = Participant::role("admin");

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
    fn context_from_request(&self, _request: &http::request::Parts) -> crate::messages::Context {
        std::iter::once(Participant::Nobody).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RoleGuard;

impl Guard for RoleGuard {
    fn context_from_request(&self, _request: &http::request::Parts) -> crate::messages::Context {
        std::iter::once(Participant::role("admin")).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ApiKeyGuard;

impl Guard for ApiKeyGuard {
    fn context_from_request(&self, _request: &http::request::Parts) -> crate::messages::Context {
        crate::messages::Context::default()
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
    api: &mut impl Service<Request<B>, Response = http::Response<String>, Error = std::convert::Infallible>,
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
    assert_eq!(body_entry["info"]["title"], "axum-bpmn API");
    assert!(body_entry["paths"]["/processes"].is_object());
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_lists_process_paths_and_raw_input_schemas() {
    let mut api = build_api();
    let (_, body) = call_json(&mut api, get("/api")).await;

    assert!(body["paths"]["/processes/start-process-1"].is_object());
    assert!(body["paths"]["/processes/start-process-1/instances"].is_object());
    assert!(body["paths"]["/processes/wait-process-1"].is_object());

    let start_schema = &body["paths"]["/processes/start-process-1"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert_eq!(start_schema["type"], "integer");

    let wait_schema = &body["paths"]["/processes/wait-process-1"]["post"]["requestBody"]["content"]
        ["application/json"]["schema"];
    assert_eq!(wait_schema["type"], "string");
    assert_eq!(wait_schema["format"], "uuid");
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_exposes_expected_component_schemas() {
    let mut api = build_api();
    let (_, body) = call_json(&mut api, get("/")).await;

    assert!(body["components"]["schemas"]["Error"].is_object());
    assert!(body["components"]["schemas"]["ProcessesResponse"].is_object());
    assert!(body["components"]["schemas"]["ProcessSummary"].is_object());
    assert!(body["components"]["schemas"]["StartedInstanceResponse"].is_object());
    assert!(body["components"]["schemas"]["Instances"].is_object());
}

#[tokio::test(flavor = "current_thread")]
async fn lists_registered_processes_with_metadata_steps_and_input_schema() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/api/processes")).await;

    assert_eq!(status, StatusCode::OK);
    let processes = body["processes"].as_array().unwrap();
    let start = processes
        .iter()
        .find(|process| process["name"] == "start-process-1")
        .unwrap();

    assert_eq!(start["metadata"]["name"], "start-process");
    assert_eq!(start["metadata"]["version"], 1);
    assert_eq!(start["input_schema"]["type"], "integer");
    assert!(
        start["steps"]
            .as_array()
            .is_some_and(|steps| steps.iter().any(|step| step == "identity"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn process_instance_routes_start_instances_on_both_aliases() {
    let mut api = build_api();

    let (status_direct, body_direct) = call_json(
        &mut api,
        post("/processes/start-process-1", serde_json::json!(12)),
    )
    .await;
    assert_eq!(status_direct, StatusCode::ACCEPTED);
    assert!(body_direct["id"].is_string());
    assert_eq!(body_direct["status"], "running");

    let (status_instances, body_instances) = call_json(
        &mut api,
        post(
            "/processes/start-process-1/instances",
            serde_json::json!(21),
        ),
    )
    .await;
    assert_eq!(status_instances, StatusCode::ACCEPTED);
    assert!(body_instances["id"].is_string());
    assert_eq!(body_instances["status"], "running");
}

#[tokio::test(flavor = "current_thread")]
async fn process_instance_routes_list_running_instances_on_both_aliases() {
    let mut api = build_api();
    let correlation_key = CorrelationKey::new();
    let (_, started) = call_json(
        &mut api,
        post(
            "/api/processes/wait-process-1/instances",
            serde_json::to_value(correlation_key).unwrap(),
        ),
    )
    .await;
    let instance_id = started["id"].as_str().unwrap().to_string();

    let (status_direct, direct_body) = call_json(&mut api, get("/processes/wait-process-1")).await;
    assert_eq!(status_direct, StatusCode::OK);
    assert!(instance_has_status(&direct_body, &instance_id, "running"));

    let (status_instances, instances_body) =
        call_json(&mut api, get("/processes/wait-process-1/instances")).await;
    assert_eq!(status_instances, StatusCode::OK);
    assert!(instance_has_status(
        &instances_body,
        &instance_id,
        "running"
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn returns_empty_instance_list_before_any_start() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/processes/start-process-1/instances")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["instances"].as_array().unwrap().len(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_invalid_json_body_syntax() {
    let mut api = build_api();
    let (status, body) = call_json(
        &mut api,
        post_raw("/processes/start-process-1/instances", "{\"broken\":"),
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
            "/processes/start-process-1/instances",
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
        call_json(&mut api, get("/processes/unknown-process-1/instances")).await;
    assert_eq!(status_unknown_process, StatusCode::NOT_FOUND);
    assert_eq!(body_unknown_process["message"], "route not found");
}

#[tokio::test(flavor = "current_thread")]
async fn unsupported_methods_return_method_not_allowed() {
    let mut api = build_api();

    let request = Request::builder()
        .method(Method::POST)
        .uri("/processes")
        .body(Full::new(bytes::Bytes::from("1")))
        .unwrap();
    let (status_post, body_post) = call_json(&mut api, request).await;
    assert_eq!(status_post, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body_post["message"], "method not allowed");

    let request = Request::builder()
        .method(Method::PUT)
        .uri("/processes/start-process-1")
        .body(Full::new(bytes::Bytes::new()))
        .unwrap();
    let (status_put, body_put) = call_json(&mut api, request).await;
    assert_eq!(status_put, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body_put["status"], 405);
}

#[tokio::test(flavor = "current_thread")]
async fn trailing_slashes_are_accepted() {
    let mut api = build_api();

    let (status_processes, _) = call_json(&mut api, get("/api/processes/")).await;
    assert_eq!(status_processes, StatusCode::OK);

    let (status_instances, _) =
        call_json(&mut api, get("/api/processes/start-process-1/instances/")).await;
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
            Participant::role("admin"),
            StatusCode::OK,
            |_, _| {
            Ok(serde_json::json!({"status":"ok"}))
            },
        )
        .build();

    let (status, body) = call_json(&mut api, get("/api/health")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["message"], "not allowed for this participant");
}

#[tokio::test(flavor = "current_thread")]
async fn guard_context_is_used_for_process_start() {
    let mut blocked = build_restricted_api(crate::EverybodyGuard);
    let (blocked_status, blocked_body) =
        call_json(&mut blocked, post("/api/processes/restricted-process-1/instances", 7.into()))
            .await;
    assert_eq!(blocked_status, StatusCode::FORBIDDEN);
    assert!(blocked_body["message"]
        .as_str()
        .is_some_and(|value| value.contains("not allowed for this participant")));

    let mut allowed = build_restricted_api(RoleGuard);
    let (allowed_status, allowed_body) =
        call_json(&mut allowed, post("/api/processes/restricted-process-1/instances", 7.into()))
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
