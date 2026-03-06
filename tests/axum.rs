use axum::{Router, routing::get};
use axum_bpmn::{Api, Runtime};

#[tokio::test(flavor = "current_thread")]
async fn test_axum() {
    let runtime = Runtime::new(axum_bpmn::executor::TokioExecutor);
    let bpnm_api = Api::new("api", runtime);
    let _: Router = Router::new()
        .route_service("/api/{*key}", bpnm_api)
        .route("/", get(|| async {}));
}
