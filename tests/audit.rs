//! Tests the protocolling of values in the process history and the roles of users.

use axum::{Router, routing::get};

use axum_test::TestServer;
use serde::Serialize;
use tower_bpmn::{
    Api,
    bpmn::{
        IncomingMessage, InstanceId, MetaData, Process, ProcessBuilder, RegisteredProcess, Runtime,
        Step,
        gateways::{And, Xor},
        messages::{CorrelationKey, Entity, Participant, Role},
        storage::{InMemory, Storage, StorageBackend},
    },
    guards::AuthorizationGuard,
};

fn instance_has_status(body: &serde_json::Value, instance_id: &str, status: &str) -> bool {
    body["instances"].as_array().is_some_and(|instances| {
        instances.iter().any(|instance| {
            instance["id"].as_str() == Some(instance_id)
                && instance["status"]
                    .as_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case(status))
        })
    })
}

async fn wait_for_generated_key(
    server: &TestServer,
    instance_id: &InstanceId,
    step_name: &str,
    auth_header: &str,
) -> CorrelationKey {
    let path = format!("/api/processes/example-process-1/steps/{step_name}/history/{instance_id}");

    let mut last_body = serde_json::json!([]);
    for _ in 0..100 {
        let response = server
            .get(&path)
            .add_header(http::header::AUTHORIZATION, auth_header)
            .await;

        assert_eq!(
            response.status_code(),
            http::StatusCode::OK,
            "history endpoint must be accessible"
        );

        let body: serde_json::Value = response.json();
        last_body = body.clone();
        if let Some(first) = body.as_array().and_then(|rows| rows.first()) {
            let raw = first["output_value"]
                .as_str()
                .expect("output_value should be a UUID string");
            return raw.parse().expect("valid generated correlation key");
        }

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    panic!(
        "generated key for step '{step_name}' was not persisted in time, last body: {last_body}"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExampleProcess;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
struct Value(pub i32);

impl Process for ExampleProcess {
    type Input = i32;
    type Output = Value;

    const INITIAL_OWNER: Participant = Participant::Role(Role::new("child"));

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new("example-process", "An example process for testing.");
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        let [process_normal, process_shortcut] = process
            .then("add_value", |_token, value| Value(value + 42))
            .split(Xor::for_splitting("check_for_shortcut", |_, value: i32| {
                if value > 50 { 0 } else { 1 }
            }));

        let sum_process = {
            let [add_1, add_2] = process_normal.split(And::new("addition splitter"));
            ProcessBuilder::join(
                And::new("addition_joiner"),
                [
                    add_1
                        .then("generate_key_1", |_, _| CorrelationKey::new())
                        .wait_for(
                            IncomingMessage::<ExampleProcess, i32>::new(ExampleProcess, "add_1")
                                .with_guard(Entity::new("max").into()),
                        ),
                    add_2
                        .then("generate_key_2", |_, _| CorrelationKey::new())
                        .wait_for(
                            IncomingMessage::<ExampleProcess, i32>::new(ExampleProcess, "add_2")
                                .with_guard(Role::new("child").into()),
                        ),
                ],
            )
            .then("sum", |token, [a, b]| {
                Value(token.get_last::<Value>().expect("value should exist").0 + a + b)
            })
        };

        ProcessBuilder::join(
            Xor::for_joining("final join"),
            [sum_process, process_shortcut],
        )
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_audit_trail() {
    let storage_backend = InMemory::default();
    let mut runtime: Runtime<tower_bpmn::executor::TokioExecutor, InMemory> = Runtime::new(
        tower_bpmn::executor::TokioExecutor::default(),
        storage_backend.clone(),
    );
    runtime
        .register_process(ExampleProcess)
        .expect("valid process");

    let bpnm_api = Api::builder("api", runtime)
        .with_openapi(true)
        .with_exposed_steps(ExampleProcess, Role::new("child").into())
        .with_guard(AuthorizationGuard::new(|username, _| match username {
            "max" => Some(
                [
                    Participant::Entity(Entity::new("max")),
                    Participant::Role(Role::new("child")),
                ]
                .into_iter()
                .collect(),
            ),
            "moritz" => Some(
                [
                    Participant::Entity(Entity::new("moritz")),
                    Participant::Role(Role::new("child")),
                ]
                .into_iter()
                .collect(),
            ),
            _ => None,
        }))
        .build();

    let app: Router = Router::new()
        .route_service("/api/{*key}", bpnm_api.clone())
        .route("/", get(|| async {}));

    let server = TestServer::new(app);

    assert_eq!(
        server
            .post("/api/processes/example-process-1/instances")
            .json(&11)
            .await
            .status_code(),
        http::StatusCode::FORBIDDEN
    );

    let start_response = server
        .post("/api/processes/example-process-1/instances")
        .add_header(http::header::AUTHORIZATION, "Basic bWF4Og==")
        .json(&60)
        .await;
    assert_eq!(start_response.status_code(), http::StatusCode::ACCEPTED);
    let start_json: serde_json::Value = start_response.json();
    let instance_id = start_json["id"].as_str().expect("instance id as string");
    let instance_id: InstanceId = instance_id.parse().expect("valid instance id");

    let (list_status, list_json): (http::StatusCode, serde_json::Value) = {
        let response = server
            .get("/api/processes/example-process-1/instances")
            .add_header(http::header::AUTHORIZATION, "Basic bWF4Og==")
            .await;
        (response.status_code(), response.json())
    };
    assert_eq!(list_status, http::StatusCode::OK);
    assert!(instance_has_status(
        &list_json,
        &instance_id.to_string(),
        "running"
    ));

    let generated_key_1 =
        wait_for_generated_key(&server, &instance_id, "generate_key_1", "Basic bWF4Og==").await;
    let generated_key_2 =
        wait_for_generated_key(&server, &instance_id, "generate_key_2", "Basic bWF4Og==").await;

    // The process is waiting on both steps and accepts payloads on both message endpoints.
    assert_eq!(
        server
            .post(&format!(
                "/api/processes/example-process-1/steps/add_1/{generated_key_1}"
            ))
            .add_header(http::header::AUTHORIZATION, "Basic bWF4Og==")
            .json(&3)
            .await
            .status_code(),
        http::StatusCode::ACCEPTED
    );
    assert_eq!(
        server
            .post(&format!(
                "/api/processes/example-process-1/steps/add_2/{generated_key_2}"
            ))
            .add_header(http::header::AUTHORIZATION, "Basic bW9yaXR6Og==")
            .json(&4)
            .await
            .status_code(),
        http::StatusCode::ACCEPTED
    );

    // Wait for the process
    let mut finished = false;
    let mut last_places: Vec<String> = Vec::new();
    for _ in 0..100 {
        let history = storage_backend
            .query_history(instance_id)
            .expect("instance history available");
        last_places = history
            .iter()
            .map(|entry| entry.place.as_str().to_string())
            .collect();
        if history.iter().any(|entry| entry.place.as_str() == "sum") {
            finished = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(
        finished,
        "process instance should eventually complete, latest places: {last_places:?}"
    );

    // Check the history of the instance and the stored values for each step.
    let history = storage_backend
        .query_history(instance_id)
        .expect("instance history available");
    assert_eq!(
        history.len(),
        11,
        "exactly 11 steps should be recorded in the history"
    );

    /*
    panic!(
        "{}",
        history
            .iter()
            .map(|entry| entry.place.as_str().to_string())
            .collect::<Vec<String>>()
            .join(", ")
    );*/

    let runtime = bpnm_api.runtime();
    let process = runtime
        .registered_processes()
        .find(|p| p.meta_data.name == "example-process")
        .expect("registered process");

    check_step(
        &storage_backend,
        process,
        instance_id,
        Step::START,
        60,
        "max",
    );

    check_step(
        &storage_backend,
        process,
        instance_id,
        "add_value",
        Value(60 + 42),
        Entity::SYSTEM.as_ref(),
    );

    check_step(
        &storage_backend,
        process,
        instance_id,
        "check_for_shortcut: 0",
        (),
        Entity::SYSTEM.as_ref(),
    );

    /*
    check_step(
        &storage_backend,
        process,
        instance_id,
        "addition splitter",
        (),
        Entity::SYSTEM.as_ref(),
    ); */

    check_step(
        &storage_backend,
        process,
        instance_id,
        "generate_key_1",
        generated_key_1,
        Entity::SYSTEM.as_ref(),
    );

    check_step(
        &storage_backend,
        process,
        instance_id,
        "generate_key_2",
        generated_key_2,
        Entity::SYSTEM.as_ref(),
    );

    check_step(&storage_backend, process, instance_id, "add_1", 3, "max");

    check_step(&storage_backend, process, instance_id, "add_2", 4, "moritz");

    check_step(
        &storage_backend,
        process,
        instance_id,
        "addition_joiner",
        [3, 4],
        Entity::SYSTEM.as_ref(),
    );

    check_step(
        &storage_backend,
        process,
        instance_id,
        "sum",
        Value(60 + 42 + 3 + 4),
        Entity::SYSTEM.as_ref(),
    );

    check_step(
        &storage_backend,
        process,
        instance_id,
        "final join",
        (),
        Entity::SYSTEM.as_ref(),
    );

    check_step(
        &storage_backend,
        process,
        instance_id,
        Step::END,
        (),
        Entity::SYSTEM.as_ref(),
    );
}

fn check_step<S: StorageBackend, T: Serialize>(
    storage_backend: &S,
    registered_process: &RegisteredProcess<S>,
    instance_id: InstanceId,
    step: &str,
    expected_output: T,
    expected_responsible: &str,
) {
    let step = registered_process
        .steps
        .get(step)
        .expect(&format!("step should exist: {}", step));
    let finished_step = {
        let mut stored_data = storage_backend
            .query(registered_process, step.clone(), instance_id)
            .expect("valid data for step");
        assert_eq!(
            stored_data.len(),
            1,
            "exactly one entry should be stored for the step"
        );
        stored_data.pop().expect("checked lenght")
    };

    assert_eq!(
        finished_step.output,
        serde_json::to_value(expected_output).expect("serializable expected output"),
        "stored output should match expected output: {step}",
    );

    assert_eq!(
        finished_step.responsible.as_ref(),
        expected_responsible,
        "stored responsible should match expected responsible: {step}",
    );
}
