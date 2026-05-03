use axum::{Router, routing::get};
use tower_bpmn::Api;
use tower_bpmn::bpmn::{Runtime, storage::InMemory};

#[tokio::test(flavor = "current_thread")]
async fn test_axum() {
    let runtime: Runtime<tower_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    let bpnm_api = Api::new("api", runtime);
    let _: Router = Router::new()
        .nest_service("/api", bpnm_api)
        .route("/", get(|| async {}));
}
