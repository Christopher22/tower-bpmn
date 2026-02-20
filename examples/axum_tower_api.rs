use std::{convert::Infallible, sync::Arc};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::Request,
    response::Response,
};
use axum_bpmn::{
    CorrelationKey, IncomingMessage, Process, ProcessBuilder, Runtime, Token,
    api::EngineApiService, api::EngineApiServiceBuilder, executor::TokioExecutor,
};
use tower::{Service, service_fn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MessageTarget;

impl Process for MessageTarget {
    type Input = i32;
    type Output = i32;

    fn name(&self) -> &str {
        "message-target-axum"
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        process.then("identity", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitForMessage;

impl Process for WaitForMessage {
    type Input = CorrelationKey;
    type Output = i32;

    fn name(&self) -> &str {
        "wait-for-message-axum"
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        process
            .wait_for(IncomingMessage::<MessageTarget, i32>::new(
                MessageTarget,
                "message-catch",
            ))
            .then("double", |_token: &Token, value| value * 2)
    }
}

fn axum_bridge(service: EngineApiService<TokioExecutor>) -> Router {
    Router::new().fallback_service(service_fn(move |request: Request<Body>| {
        let service = service.clone();
        async move {
            let (parts, body) = request.into_parts();
            let bytes = to_bytes(body, usize::MAX)
                .await
                .expect("request body should be readable");

            let mut service = service;
            let response = service
                .call(Request::from_parts(parts, bytes.to_vec()))
                .await
                .expect("engine api service is infallible");

            let (parts, body) = response.into_parts();
            let response = Response::from_parts(parts, Body::from(body));
            Ok::<_, Infallible>(response)
        }
    }))
}

#[tokio::main]
async fn main() {
    let mut runtime = Runtime::new(TokioExecutor);
    runtime
        .register_process(MessageTarget)
        .expect("register message target process");
    runtime
        .register_process(WaitForMessage)
        .expect("register wait process");

    let service = EngineApiServiceBuilder::new(Arc::new(runtime))
        .expose_start(WaitForMessage)
        .expose_message::<MessageTarget, i32>(MessageTarget)
        .build();

    let app = axum_bridge(service);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("bind listener");

    println!("API listening on http://127.0.0.1:3000");
    println!("OpenAPI: http://127.0.0.1:3000/openapi.json");

    axum::serve(listener, app).await.expect("server should run");
}
