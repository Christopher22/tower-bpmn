use axum::{Router, routing::get};
use axum_bpmn::{Api, InMemory, Runtime};

#[tokio::test(flavor = "current_thread")]
async fn test_axum() {
    let runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    let bpnm_api = Api::new("api", runtime);
    let _: Router = Router::new()
        .route_service("/api/{*key}", bpnm_api)
        .route("/", get(|| async {}));
}
