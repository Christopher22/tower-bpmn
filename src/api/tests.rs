use http::{Method, Request, StatusCode};
use http_body_util::{Empty, Full};
use tower_service::Service;

use crate::{
    Api, CorrelationKey, IncomingMessage, MetaData, Process, ProcessBuilder, Runtime, Token,
    executor::TokioExecutor,
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

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        process.then("identity", |_token: &Token, value| value)
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

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
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

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        process
            .wait_for(IncomingMessage::<MessageTarget, i32>::new(
                MessageTarget,
                "incoming",
            ))
            .then("double", |_token, value| value * 2)
    }
}

fn build_api() -> Api<TokioExecutor> {
    let mut runtime = Runtime::new(TokioExecutor);
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

async fn call_json<B>(
    api: &mut Api<TokioExecutor>,
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

#[tokio::test(flavor = "current_thread")]
async fn openapi_root_is_available() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/api/")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["openapi"], "3.1.0");
    assert!(body["paths"]["/processes"].is_object());
    assert!(body["components"]["schemas"]["StartInstanceResponse"].is_object());
    assert!(body["components"]["schemas"]["ProcessInstancesResponse"].is_object());
    assert!(body["components"]["schemas"]["RuntimeInstance"].is_object());
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_lists_registered_process_paths_and_input_schemas() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/api/")).await;

    assert_eq!(status, StatusCode::OK);

    assert!(body["paths"]["/processes/start-process/instances"].is_object());
    assert!(body["paths"]["/processes/wait-process/instances"].is_object());
    assert!(body["paths"]["/processes/message-target/messages"].is_object());
    assert!(body["paths"]["/processes/{process_name}"].is_object());

    let start_input_schema = &body["paths"]["/processes/start-process/instances"]["post"]["requestBody"]
        ["content"]["application/json"]["schema"]["properties"]["input"];
    assert_eq!(start_input_schema["type"], "integer");

    let wait_input_schema = &body["paths"]["/processes/wait-process/instances"]["post"]["requestBody"]
        ["content"]["application/json"]["schema"]["properties"]["input"];
    assert_eq!(wait_input_schema["type"], "string");
    assert_eq!(wait_input_schema["format"], "uuid");
}

#[tokio::test(flavor = "current_thread")]
async fn openapi_uses_exact_response_struct_refs() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/api/")).await;

    assert_eq!(status, StatusCode::OK);

    assert_eq!(
        body["paths"]["/processes/start-process/instances"]["post"]["responses"]["202"]["content"]
            ["application/json"]["schema"]["$ref"],
        "#/components/schemas/StartInstanceResponse"
    );
    assert_eq!(
        body["paths"]["/processes/start-process/instances"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ProcessInstancesResponse"
    );
    assert_eq!(
        body["paths"]["/processes/message-target/messages"]["post"]["responses"]["202"]["content"]
            ["application/json"]["schema"]["$ref"],
        "#/components/schemas/AcceptedResponse"
    );
    assert_eq!(
        body["paths"]["/instances/{instance_id}"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/RuntimeInstance"
    );
    assert_eq!(
        body["paths"]["/processes/{process_name}"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/ProcessMetadataResponse"
    );

    let process_entries = body["x-registered-processes"]
        .as_array()
        .expect("registered process metadata should be an array");
    assert_eq!(process_entries.len(), 3);
    assert!(
        process_entries
            .iter()
            .any(|entry| entry["name"] == "start-process")
    );
    assert!(
        process_entries
            .iter()
            .any(|entry| entry["name"] == "wait-process")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn lists_registered_processes() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/processes")).await;

    assert_eq!(status, StatusCode::OK);
    let names = body["processes"].as_array().unwrap();
    assert!(names.iter().any(|name| name == "start-process"));
    assert!(names.iter().any(|name| name == "wait-process"));
}

#[tokio::test(flavor = "current_thread")]
async fn returns_registered_process_metadata() {
    let mut api = build_api();
    let (status, body) = call_json(&mut api, get("/processes/start-process")).await;

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
    let (status, body) = call_json(&mut api, get("/processes/unknown-process")).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "unknown process");
}

#[tokio::test(flavor = "current_thread")]
async fn starts_new_process_instances() {
    let mut api = build_api();
    let request = post(
        "/processes/start-process/instances",
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
            "/processes/wait-process/instances",
            serde_json::json!({ "input": correlation_key }),
        ),
    )
    .await;

    assert_eq!(start_status, StatusCode::ACCEPTED);
    let instance_id = start_body["instance_id"].as_str().unwrap().to_string();

    let (list_status, list_body) =
        call_json(&mut api, get("/processes/wait-process/instances")).await;
    assert_eq!(list_status, StatusCode::OK);
    let instances = list_body["instances"].as_array().unwrap();
    assert!(
        instances
            .iter()
            .any(|instance| instance["id"] == instance_id && instance["status"] == "running")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn sends_messages_to_waiting_instances() {
    let mut api = build_api();
    let correlation_key = CorrelationKey::new();

    let (_, start_body) = call_json(
        &mut api,
        post(
            "/processes/wait-process/instances",
            serde_json::json!({ "input": correlation_key }),
        ),
    )
    .await;
    let instance_id = start_body["instance_id"].as_str().unwrap().to_string();

    let (message_status, message_body) = call_json(
        &mut api,
        post(
            "/processes/message-target/messages",
            serde_json::json!({
                "correlation_key": correlation_key,
                "payload": 21
            }),
        ),
    )
    .await;

    assert_eq!(message_status, StatusCode::ACCEPTED);
    assert_eq!(message_body["status"], "accepted");

    for _ in 0..20 {
        let (_, instance_body) =
            call_json(&mut api, get(&format!("/instances/{instance_id}"))).await;
        if instance_body["status"] == "completed" {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    panic!("instance did not reach completed status in time");
}
