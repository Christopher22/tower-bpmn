use http::{Method, Request, StatusCode};
use http_body_util::{Empty, Full};
use tower_service::Service;

use crate::{
    Api, InMemory, IncomingMessage, MetaData, Process, ProcessBuilder, Runtime, Storage, Token,
    executor::TokioExecutor, messages::CorrelationKey,
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

fn build_api() -> Api<TokioExecutor, InMemory> {
    let mut runtime = Runtime::default();
    runtime.register_process(StartProcess).unwrap();
    runtime.register_process(MessageTarget).unwrap();
    runtime.register_process(WaitProcess).unwrap();
    Api::new("api", runtime)
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

fn post_raw(path: &str, body: &str, content_type: &str) -> Request<Full<bytes::Bytes>> {
    Request::builder()
        .method(Method::POST)
        .uri(path)
        .header("content-type", content_type)
        .body(Full::new(bytes::Bytes::from(body.to_string())))
        .unwrap()
}

async fn call_json<B>(
    api: &mut Api<TokioExecutor, InMemory>,
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

async fn openapi_doc(api: &mut Api<TokioExecutor, InMemory>) -> serde_json::Value {
    let (status, body) = call_json(api, get("/api/")).await;
    assert_eq!(status, StatusCode::OK);
    body
}

fn has_status(status_value: &serde_json::Value, expected: &str) -> bool {
    match status_value {
        serde_json::Value::String(value) => value.eq_ignore_ascii_case(expected),
        serde_json::Value::Object(value) => {
            value.keys().any(|key| key.eq_ignore_ascii_case(expected))
        }
        _ => false,
    }
}

fn instance_matches_status(
    instance: &serde_json::Value,
    instance_id: &str,
    expected: &str,
) -> bool {
    instance["id"] == instance_id && has_status(&instance["status"], expected)
}

async fn wait_for_instance_status(
    api: &mut Api<TokioExecutor, InMemory>,
    process_name: &str,
    instance_id: &str,
    expected: &str,
) -> bool {
    for _ in 0..50 {
        let (status, body) =
            call_json(api, get(&format!("/processes/{process_name}/instances"))).await;
        if status == StatusCode::OK
            && body["instances"].as_array().is_some_and(|instances| {
                instances
                    .iter()
                    .any(|instance| instance_matches_status(instance, instance_id, expected))
            })
        {
            return true;
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    false
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_root_is_available() {
    let mut api = build_api();
    let body = openapi_doc(&mut api).await;

    assert_eq!(body["openapi"], "3.1.0");
    assert!(body["paths"]["/processes"].is_object());
    assert!(body["components"]["schemas"]["StartInstanceResponse"].is_object());
    assert!(body["components"]["schemas"]["Instances"].is_object());
    assert!(body["components"]["schemas"]["Instance"].is_object());
    assert!(body["components"]["schemas"]["AcceptedResponse"].is_object());
    assert!(body["components"]["schemas"]["ProcessMetadataResponse"].is_object());
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_lists_registered_process_paths_and_input_schemas() {
    let mut api = build_api();
    let body = openapi_doc(&mut api).await;

    assert!(body["paths"]["/processes/start-process-1/instances"].is_object());
    assert!(body["paths"]["/processes/wait-process-1/instances"].is_object());
    assert!(body["paths"]["/processes/message-target-1/messages"].is_object());
    assert!(body["paths"]["/processes/start-process-1/"].is_object());

    let start_input_schema = &body["paths"]["/processes/start-process-1/instances"]["post"]["requestBody"]
        ["content"]["application/json"]["schema"]["properties"]["input"];
    assert_eq!(start_input_schema["type"], "integer");

    let wait_input_schema = &body["paths"]["/processes/wait-process-1/instances"]["post"]["requestBody"]
        ["content"]["application/json"]["schema"]["properties"]["input"];
    assert_eq!(wait_input_schema["type"], "string");
    assert_eq!(wait_input_schema["format"], "uuid");
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_uses_exact_response_struct_refs() {
    let mut api = build_api();
    let body = openapi_doc(&mut api).await;

    assert_eq!(
        body["paths"]["/processes/start-process-1/instances"]["post"]["responses"]["202"]["content"]
            ["application/json"]["schema"]["$ref"],
        "#/components/schemas/StartInstanceResponse"
    );
    assert_eq!(
        body["paths"]["/processes/start-process-1/instances"]["get"]["responses"]["200"]["content"]
            ["application/json"]["schema"]["$ref"],
        "#/components/schemas/Instances"
    );
    assert_eq!(
        body["paths"]["/processes/message-target-1/messages"]["post"]["responses"]["202"]["content"]
            ["application/json"]["schema"]["$ref"],
        "#/components/schemas/AcceptedResponse"
    );
    assert_eq!(
        body["paths"]["/processes/start-process-1/"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ProcessMetadataResponse"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn lists_registered_processes() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/processes")).await;

    assert_eq!(status, StatusCode::OK);
    let names = body["processes"].as_array().unwrap();
    assert!(names.iter().any(|name| name == "start-process-1"));
    assert!(names.iter().any(|name| name == "wait-process-1"));
}

#[tokio::test(flavor = "current_thread")]
async fn returns_registered_process_metadata() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/processes/start-process-1")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["process"]["name"], "start-process");
    assert_eq!(body["process"]["version"], 1);
    assert_eq!(
        body["process"]["description"],
        "Starts and completes immediately"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn returns_not_found_for_unknown_process_metadata() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/processes/unknown-process-1")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "unknown process");
}

#[tokio::test(flavor = "current_thread")]
async fn starts_new_process_instances() {
    let mut api = build_api();
    let request = post(
        "/processes/start-process-1/instances",
        serde_json::json!({ "input": 12 }),
    );
    let (status, body) = call_json(&mut api, request).await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body["instance_id"].is_string());
}

#[tokio::test(flavor = "current_thread")]
async fn lists_running_instances_and_can_read_instance_state() {
    let mut api = build_api();
    let correlation_key = CorrelationKey::new();

    let (start_status, start_body) = call_json(
        &mut api,
        post(
            "/processes/wait-process-1/instances",
            serde_json::json!({ "input": correlation_key }),
        ),
    )
    .await;

    assert_eq!(start_status, StatusCode::ACCEPTED);
    let instance_id = start_body["instance_id"].as_str().unwrap().to_string();

    let (list_status, list_body) =
        call_json(&mut api, get("/processes/wait-process-1/instances")).await;
    assert_eq!(list_status, StatusCode::OK);
    let instances = list_body["instances"].as_array().unwrap();
    assert!(instances.iter().any(|instance| instance_matches_status(
        instance,
        &instance_id,
        "running"
    )));
    assert!(wait_for_instance_status(&mut api, "wait-process-1", &instance_id, "running").await);
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_invalid_json_payloads() {
    let mut api = build_api();

    let (status, body) = call_json(
        &mut api,
        post_raw(
            "/processes/start-process-1/instances",
            "{\"input\": 42",
            "application/json",
        ),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|msg| msg.contains("invalid JSON body"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rejects_invalid_start_instance_payload_shape() {
    let mut api = build_api();

    let (status, body) = call_json(
        &mut api,
        post(
            "/processes/start-process-1/instances",
            serde_json::json!({ "input": "not-an-integer" }),
        ),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|msg| msg.contains("invalid input"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn returns_not_found_for_unknown_process_instances_route() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/processes/unknown-process-1/instances")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "unknown process");
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_routes_return_not_found() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/unknown-route")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "route not found");
}
