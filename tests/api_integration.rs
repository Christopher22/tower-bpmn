use std::{convert::Infallible, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use axum_bpmn::{
    CorrelationKey, IncomingMessage, Process, ProcessBuilder, Runtime, Token,
    api::EngineApiService, api::EngineApiServiceBuilder, executor::TokioExecutor,
};
use serde_json::json;
use tower::{Service, ServiceExt, service_fn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MessageTargetApi;

impl Process for MessageTargetApi {
    type Input = i32;
    type Output = i32;

    fn name(&self) -> &str {
        "message-target-api"
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        process.then("identity", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitForMessageApi;

impl Process for WaitForMessageApi {
    type Input = CorrelationKey;
    type Output = i32;

    fn name(&self) -> &str {
        "wait-for-message-api"
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        process
            .wait_for(IncomingMessage::<MessageTargetApi, i32>::new(
                MessageTargetApi,
                "message-catch",
            ))
            .then("double", |_token: &Token, value| value * 2)
    }
}

fn build_axum_router(service: EngineApiService<TokioExecutor>) -> Router {
    Router::new().fallback_service(service_fn(move |request: Request<Body>| {
        let service = service.clone();
        async move {
            let (parts, body) = request.into_parts();
            let body = to_bytes(body, usize::MAX)
                .await
                .expect("request body should be readable");

            let mut service = service;
            let request = Request::from_parts(parts, body.to_vec());
            let response = service
                .call(request)
                .await
                .expect("engine api service is infallible");

            let (parts, body) = response.into_parts();
            let response = http::Response::from_parts(parts, Body::from(body));
            Ok::<_, Infallible>(response)
        }
    }))
}

async fn send_request(
    router: &Router,
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let builder = Request::builder().method(method).uri(path);
    let request = match body {
        Some(value) => builder
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(value.to_string()))
            .expect("request should build"),
        None => builder.body(Body::empty()).expect("request should build"),
    };

    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("router request should succeed");

    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");
    let value = if bytes.is_empty() {
        json!(null)
    } else {
        serde_json::from_slice(&bytes).expect("response JSON should be valid")
    };

    (status, value)
}

#[tokio::test(flavor = "current_thread")]
async fn api_exposes_processes_instances_messages_and_openapi() {
    let mut runtime = Runtime::new(TokioExecutor);
    runtime
        .register_process(MessageTargetApi)
        .expect("message target registration should succeed");
    runtime
        .register_process(WaitForMessageApi)
        .expect("wait process registration should succeed");

    let service = EngineApiServiceBuilder::new(Arc::new(runtime))
        .expose_start(WaitForMessageApi)
        .expose_message::<MessageTargetApi, i32>(MessageTargetApi)
        .build();

    let router = build_axum_router(service);

    let (status, processes) = send_request(&router, Method::GET, "/processes", None).await;
    assert_eq!(status, StatusCode::OK);
    let processes = processes
        .as_array()
        .expect("process list should be an array");
    assert!(
        processes.iter().any(
            |process| process["name"] == "wait-for-message-api" && process["can_start"] == true
        )
    );
    assert!(
        processes
            .iter()
            .any(|process| process["name"] == "message-target-api"
                && process["can_receive_messages"] == true)
    );

    let key = CorrelationKey::new();
    let (status, started) = send_request(
        &router,
        Method::POST,
        "/processes/wait-for-message-api/start",
        Some(json!({ "payload": key })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let instance_id = started["instance_id"]
        .as_str()
        .expect("start response should contain instance_id")
        .to_string();

    let (status, running) = send_request(&router, Method::GET, "/instances/running", None).await;
    assert_eq!(status, StatusCode::OK);
    let running = running
        .as_array()
        .expect("running instances response should be an array");
    assert!(
        running
            .iter()
            .any(|instance| { instance["id"] == instance_id && instance["status"] == "running" })
    );

    let (status, _response) = send_request(
        &router,
        Method::POST,
        "/processes/message-target-api/messages",
        Some(json!({
            "correlation_key": key,
            "payload": 12
        })),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (status, openapi) = send_request(&router, Method::GET, "/openapi.json", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(openapi["paths"]["/processes/{process}/start"].is_object());
    assert!(openapi["components"]["schemas"]["CorrelationKey"].is_object());
    assert!(openapi["components"]["schemas"]["i32"].is_object());
}

#[tokio::test(flavor = "current_thread")]
async fn api_returns_bad_request_for_invalid_payload() {
    let mut runtime = Runtime::new(TokioExecutor);
    runtime
        .register_process(WaitForMessageApi)
        .expect("wait process registration should succeed");

    let service = EngineApiServiceBuilder::new(Arc::new(runtime))
        .expose_start(WaitForMessageApi)
        .build();
    let router = build_axum_router(service);

    let (status, body) = send_request(
        &router,
        Method::POST,
        "/processes/wait-for-message-api/start",
        Some(json!({ "payload": 42 })),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .expect("error response should contain a message")
            .contains("invalid payload")
    );
}
