use axum_bpmn::{
    InMemory, IncomingMessage, Process, ProcessBuilder, Runtime, Step, Storage, Token,
    messages::{Context, CorrelationKey, Message},
};
use std::time::Duration;

// ============================================================================
// PROCESS DEFINITIONS
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MessageTarget;

impl Process for MessageTarget {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &axum_bpmn::MetaData {
        static META: axum_bpmn::MetaData = axum_bpmn::MetaData::new(
            "message-target-compat",
            "A process that demonstrates message targeting.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process.then("identity", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitForMessageProcess;

impl Process for WaitForMessageProcess {
    type Input = CorrelationKey;
    type Output = i32;

    fn metadata(&self) -> &axum_bpmn::MetaData {
        static META: axum_bpmn::MetaData = axum_bpmn::MetaData::new(
            "wait-for-message-compat",
            "A process that demonstrates waiting for messages.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process
            .wait_for(IncomingMessage::<MessageTarget, i32>::new(
                MessageTarget,
                "message-catch-event",
            ))
            .then("post-process", |_token, value| value * 3)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ThrowMessageProcess;

impl Process for ThrowMessageProcess {
    type Input = (CorrelationKey, i32);
    type Output = i32;

    fn metadata(&self) -> &axum_bpmn::MetaData {
        static META: axum_bpmn::MetaData = axum_bpmn::MetaData::new(
            "throw-message-compat",
            "A process that demonstrates throwing messages.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process
            .throw_message(
                "message-throw-event",
                MessageTarget,
                |_token: &Token<S>, (correlation_key, payload): (CorrelationKey, i32)| Message {
                    process: MessageTarget,
                    payload,
                    correlation_key,
                    context: Context::default(),
                },
            )
            .then("complete", |_token, (_key, payload)| payload)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParallelAggregationProcess;

impl Process for ParallelAggregationProcess {
    type Input = i32;
    type Output = [i32; 2];

    fn metadata(&self) -> &axum_bpmn::MetaData {
        static META: axum_bpmn::MetaData = axum_bpmn::MetaData::new(
            "parallel-aggregation-compat",
            "A process that demonstrates parallel aggregation.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        let [left, right] = process.split(axum_bpmn::gateways::And("AND Split".into()));
        ProcessBuilder::join(
            axum_bpmn::gateways::And("Join path".into()),
            [
                left.then("left-path", |_token, value| value + 10),
                right.then("right-path", |_token, value| value + 20),
            ],
        )
    }
}

// --- Additional Processes for Extended Test Coverage ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SimpleSequentialProcess;

impl Process for SimpleSequentialProcess {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &axum_bpmn::MetaData {
        static META: axum_bpmn::MetaData = axum_bpmn::MetaData::new(
            "simple-sequential",
            "A standard multi-step sequential process.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process
            .then("step-1-add", |_token, v| v + 5)
            .then("step-2-mul", |_token, v| v * 2)
            .then("step-3-sub", |_token, v| v - 4)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TypeConversionProcess;

impl Process for TypeConversionProcess {
    type Input = i32;
    type Output = String;

    fn metadata(&self) -> &axum_bpmn::MetaData {
        static META: axum_bpmn::MetaData = axum_bpmn::MetaData::new(
            "type-conversion",
            "Demonstrates type transitions across sequence flows.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        process.then("convert-to-string", |_token, value| format!("ID-{}", value))
    }
}

// ============================================================================
// 2. ADVANCED PROCESS DEFINITIONS (Gateways & Lifecycle)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExclusiveGatewayProcess;

impl Process for ExclusiveGatewayProcess {
    type Input = i32;
    type Output = String;

    fn metadata(&self) -> &axum_bpmn::MetaData {
        static META: axum_bpmn::MetaData = axum_bpmn::MetaData::new(
            "exclusive-gateway-process",
            "Tests XOR conditional routing logic.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        // Assuming a standard conditional API for the Exclusive gateway
        let [high_path, low_path] = process.split(axum_bpmn::gateways::Xor::for_splitting(
            "Check size",
            |_, v| match v {
                ..=100 => 1,
                _ => 0,
            },
        ));

        ProcessBuilder::join(
            axum_bpmn::gateways::Xor::for_joining("Estimate result result"),
            [
                high_path.then("high-path", |_token, _| "HIGH".to_string()),
                low_path.then("low-path", |_token, _| "LOW".to_string()),
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NestedGatewayProcess;

impl Process for NestedGatewayProcess {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &axum_bpmn::MetaData {
        static META: axum_bpmn::MetaData = axum_bpmn::MetaData::new(
            "nested-gateway-process",
            "Tests AND gateway inside another AND gateway.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        process: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        let [outer_left, outer_right] = process.split(axum_bpmn::gateways::And("Outer AND".into()));

        // Nesting an AND gateway inside the left branch
        let [inner_left, inner_right] =
            outer_left.split(axum_bpmn::gateways::And("Inner AND".into()));
        let inner_join = ProcessBuilder::join(
            axum_bpmn::gateways::And("Inner Join".into()),
            [
                inner_left.then("inner-add-1", |_token, v| v + 1),
                inner_right.then("inner-add-2", |_token, v| v + 2),
            ],
        )
        .then("sum-inner", |_token, [a, b]| a + b); // Expected: (v+1) + (v+2)

        let outer_right_path = outer_right.then("outer-mul", |_token, v| v * 10);

        ProcessBuilder::join(
            axum_bpmn::gateways::And("Final Join".into()),
            [inner_join, outer_right_path],
        )
        .then("final-sum", |_token, [inner_res, outer_res]| {
            inner_res + outer_res
        })
    }
}

// ============================================================================
// UNIT TESTS
// ============================================================================

// --- 1. Metadata and Initialization Tests ---

#[test]
fn test_metadata_message_target() {
    let process = MessageTarget;
    assert_eq!(process.metadata().name, "message-target-compat");
}

#[test]
fn test_metadata_wait_for_message() {
    let process = WaitForMessageProcess;
    assert_eq!(process.metadata().name, "wait-for-message-compat");
}

#[test]
fn test_metadata_throw_message() {
    let process = ThrowMessageProcess;
    assert_eq!(process.metadata().name, "throw-message-compat");
}

#[test]
fn test_metadata_parallel_aggregation() {
    let process = ParallelAggregationProcess;
    assert_eq!(process.metadata().name, "parallel-aggregation-compat");
}

#[tokio::test(flavor = "current_thread")]
async fn test_engine_registry_allows_multiple_registrations() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    assert!(runtime.register_process(MessageTarget).is_ok());
    assert!(runtime.register_process(WaitForMessageProcess).is_ok());
    assert!(runtime.register_process(ThrowMessageProcess).is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_unregistered_process_fails_to_start() {
    let runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    // Intentionally skipping registration
    let result = runtime.run(SimpleSequentialProcess, 10);
    assert!(
        result.is_err(),
        "Engine should reject running unregistered processes to maintain strict registry integrity."
    );
}

// --- 2. Sequential Flow & Data Handling Tests ---

#[tokio::test(flavor = "current_thread")]
async fn test_sequential_execution_correctness() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(SimpleSequentialProcess).unwrap();

    let instance = runtime.run(SimpleSequentialProcess, 10).unwrap();
    let token = runtime
        .wait_for_completion(&SimpleSequentialProcess, instance)
        .await
        .unwrap()
        .unwrap();

    // Math check: (10 + 5) * 2 - 4 = 26
    assert_eq!(token.get_last::<i32>(), Some(26));
}

#[tokio::test(flavor = "current_thread")]
async fn test_sequential_execution_last_step() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(SimpleSequentialProcess).unwrap();

    let instance = runtime.run(SimpleSequentialProcess, 0).unwrap();
    let token = runtime
        .wait_for_completion(&SimpleSequentialProcess, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.last_step().unwrap().as_str(), Step::END);
}

#[tokio::test(flavor = "current_thread")]
async fn test_type_conversion_process() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(TypeConversionProcess).unwrap();

    let instance = runtime.run(TypeConversionProcess, 404).unwrap();
    let token = runtime
        .wait_for_completion(&TypeConversionProcess, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<String>(), Some("ID-404".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn test_token_get_last() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(TypeConversionProcess).unwrap();

    let instance = runtime.run(TypeConversionProcess, 200).unwrap();
    let token = runtime
        .wait_for_completion(&TypeConversionProcess, instance)
        .await
        .unwrap()
        .unwrap();

    // The output is a String, requesting an i32 should return the value before
    assert_eq!(token.get_last::<i32>(), Some(200));
    assert_eq!(token.get_last::<String>(), Some("ID-200".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn test_message_target_identity_behavior() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(MessageTarget).unwrap();

    let instance = runtime.run(MessageTarget, 99).unwrap();
    let token = runtime
        .wait_for_completion(&MessageTarget, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<i32>(), Some(99));
    assert_eq!(token.last_step().unwrap().as_str(), Step::END);
}

// --- 3. Parallel Gateway Tests ---

#[tokio::test(flavor = "current_thread")]
async fn test_parallel_gateway_join_produces_combined_data_object() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime
        .register_process(ParallelAggregationProcess)
        .unwrap();

    let instance = runtime.run(ParallelAggregationProcess, 3).unwrap();
    let token = runtime
        .wait_for_completion(&ParallelAggregationProcess, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<[i32; 2]>(), Some([13, 23]));
    assert_eq!(token.last_step().unwrap().as_str(), Step::END);
}

#[tokio::test(flavor = "current_thread")]
async fn test_parallel_aggregation_with_negative_inputs() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime
        .register_process(ParallelAggregationProcess)
        .unwrap();

    let instance = runtime.run(ParallelAggregationProcess, -50).unwrap();
    let token = runtime
        .wait_for_completion(&ParallelAggregationProcess, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<[i32; 2]>(), Some([-40, -30]));
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_concurrent_parallel_aggregations() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime
        .register_process(ParallelAggregationProcess)
        .unwrap();

    let mut handles = Vec::new();
    for i in 0..5 {
        let instance = runtime.run(ParallelAggregationProcess, i).unwrap();
        handles.push(runtime.wait_for_completion(&ParallelAggregationProcess, instance));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        let token = handle.await.unwrap().unwrap();
        let i = i as i32;
        assert_eq!(token.get_last::<[i32; 2]>(), Some([i + 10, i + 20]));
    }
}

// --- 4. Messaging & Correlation Tests ---

#[test]
fn test_correlation_key_uniqueness() {
    let key1 = CorrelationKey::new();
    let key2 = CorrelationKey::new();
    assert_ne!(
        key1, key2,
        "Correlation keys must generate uniquely to prevent instance leakage."
    );
    assert_eq!(key1, key1.clone());
}

#[tokio::test(flavor = "current_thread")]
async fn test_throw_then_catch_message_event_with_correlation() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(ThrowMessageProcess).unwrap();
    runtime.register_process(WaitForMessageProcess).unwrap();

    let key = CorrelationKey::new();
    let thrower = runtime.run(ThrowMessageProcess, (key, 14)).unwrap();
    let waiter = runtime.run(WaitForMessageProcess, key).unwrap();

    let thrower_token = runtime
        .wait_for_completion(&ThrowMessageProcess, thrower)
        .await
        .unwrap()
        .unwrap();
    let waiter_token = runtime
        .wait_for_completion(&WaitForMessageProcess, waiter)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(thrower_token.get_last::<i32>(), Some(14));
    assert_eq!(waiter_token.get_last::<i32>(), Some(42));
}

#[tokio::test(flavor = "current_thread")]
async fn test_correlation_keys_isolate_parallel_message_instances() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(WaitForMessageProcess).unwrap();

    let key_a = CorrelationKey::new();
    let key_b = CorrelationKey::new();

    let waiter_a = runtime.run(WaitForMessageProcess, key_a).unwrap();
    let waiter_b = runtime.run(WaitForMessageProcess, key_b).unwrap();

    runtime
        .messages
        .send_message(Message {
            process: MessageTarget,
            payload: 7,
            correlation_key: key_b,
            context: Context::default(),
        })
        .unwrap();
    runtime
        .messages
        .send_message(Message {
            process: MessageTarget,
            payload: 5,
            correlation_key: key_a,
            context: Context::default(),
        })
        .unwrap();

    let (token_a, token_b) = tokio::join!(
        runtime.wait_for_completion(&WaitForMessageProcess, waiter_a),
        runtime.wait_for_completion(&WaitForMessageProcess, waiter_b)
    );

    assert_eq!(token_a.unwrap().unwrap().get_last::<i32>(), Some(15));
    assert_eq!(token_b.unwrap().unwrap().get_last::<i32>(), Some(21));
}

#[tokio::test(flavor = "current_thread")]
async fn test_message_catch_with_early_send_buffers_correctly() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(WaitForMessageProcess).unwrap();

    let key = CorrelationKey::new();

    // Simulate BPMN spec: incoming messages before the catch event is active should be buffered securely.
    runtime
        .messages
        .send_message(Message {
            process: MessageTarget,
            payload: 10,
            correlation_key: key,
            context: Context::default(),
        })
        .unwrap();

    let waiter = runtime.run(WaitForMessageProcess, key).unwrap();
    let token = runtime
        .wait_for_completion(&WaitForMessageProcess, waiter)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<i32>(), Some(30)); // 10 * 3
}

#[tokio::test(flavor = "current_thread")]
async fn test_throw_message_process_payload_extraction() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(ThrowMessageProcess).unwrap();

    let key = CorrelationKey::new();
    let thrower = runtime.run(ThrowMessageProcess, (key, 100)).unwrap();

    let token = runtime
        .wait_for_completion(&ThrowMessageProcess, thrower)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(token.get_last::<i32>(), Some(100)); // The throw process natively outputs its payload
    assert_eq!(token.last_step().unwrap().as_str(), Step::END);
}

#[tokio::test(flavor = "current_thread")]
async fn test_wait_for_message_post_processing_logic() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(WaitForMessageProcess).unwrap();

    let key = CorrelationKey::new();
    let waiter = runtime.run(WaitForMessageProcess, key).unwrap();

    runtime
        .messages
        .send_message(Message {
            process: MessageTarget,
            payload: 11, // Expect * 3 post-process
            correlation_key: key,
            context: Context::default(),
        })
        .unwrap();

    let token = runtime
        .wait_for_completion(&WaitForMessageProcess, waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(token.last_step().unwrap().as_str(), Step::END);
    assert_eq!(token.get_last::<i32>(), Some(33));
}

// --- 5. Complex Concurrency Simulation ---

#[tokio::test(flavor = "current_thread")]
async fn test_complex_concurrent_workflow_orchestration() {
    // Ensuring the engine handles multiple disconnected processes concurrently without cross-contamination.
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(ThrowMessageProcess).unwrap();
    runtime.register_process(WaitForMessageProcess).unwrap();
    runtime
        .register_process(ParallelAggregationProcess)
        .unwrap();

    let key = CorrelationKey::new();

    // Kick off 3 completely different instances
    let thrower = runtime.run(ThrowMessageProcess, (key, 4)).unwrap();
    let waiter = runtime.run(WaitForMessageProcess, key).unwrap();
    let parallel = runtime.run(ParallelAggregationProcess, 5).unwrap();

    let (throw_t, wait_t, par_t) = tokio::join!(
        runtime.wait_for_completion(&ThrowMessageProcess, thrower),
        runtime.wait_for_completion(&WaitForMessageProcess, waiter),
        runtime.wait_for_completion(&ParallelAggregationProcess, parallel)
    );

    assert_eq!(throw_t.unwrap().unwrap().get_last::<i32>(), Some(4));
    assert_eq!(wait_t.unwrap().unwrap().get_last::<i32>(), Some(12)); // 4 * 3
    assert_eq!(
        par_t.unwrap().unwrap().get_last::<[i32; 2]>(),
        Some([15, 25])
    ); // 5+10, 5+20
}

// --- Gateway Routing & Nesting Tests ---

#[tokio::test(flavor = "current_thread")]
async fn test_exclusive_gateway_routes_high_condition() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(ExclusiveGatewayProcess).unwrap();

    let instance = runtime.run(ExclusiveGatewayProcess, 150).unwrap();
    let token = runtime
        .wait_for_completion(&ExclusiveGatewayProcess, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<String>(), Some("HIGH".to_string()));
    assert_eq!(token.last_step().unwrap().as_str(), Step::END);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exclusive_gateway_routes_low_condition() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(ExclusiveGatewayProcess).unwrap();

    let instance = runtime.run(ExclusiveGatewayProcess, 50).unwrap();
    let token = runtime
        .wait_for_completion(&ExclusiveGatewayProcess, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<String>(), Some("LOW".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_parallel_gateways_synchronize_correctly() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(NestedGatewayProcess).unwrap();

    // Input: 5
    // Inner left: 5+1=6, Inner right: 5+2=7 -> Sum inner: 13
    // Outer right: 5*10=50
    // Final sum: 13 + 50 = 63
    let instance = runtime.run(NestedGatewayProcess, 5).unwrap();
    let token = runtime
        .wait_for_completion(&NestedGatewayProcess, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<i32>(), Some(63));
}

// --- Process Lifecycle & Termination Tests ---

#[tokio::test(flavor = "current_thread")]
async fn test_process_suspends_and_does_not_terminate_prematurely() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(WaitForMessageProcess).unwrap();

    let key = CorrelationKey::new();
    let waiter = runtime.run(WaitForMessageProcess, key).unwrap();

    // Wait a brief moment to ensure the engine doesn't falsely complete the process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // We attempt to poll for completion with a timeout. It should timeout because it's still waiting.
    let timeout_result = tokio::time::timeout(
        Duration::from_millis(10),
        runtime.wait_for_completion(&WaitForMessageProcess, waiter),
    )
    .await;

    assert!(
        timeout_result.is_err(),
        "Engine falsely reported completion while process should be suspended waiting for a message event."
    );

    // Now fulfill the process
    runtime
        .messages
        .send_message(Message {
            process: MessageTarget,
            payload: 10,
            correlation_key: key,
            context: Context::default(),
        })
        .unwrap();

    let token = runtime
        .wait_for_completion(&WaitForMessageProcess, waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(token.get_last::<i32>(), Some(30)); // Confirm proper final termination
}

#[tokio::test(flavor = "current_thread")]
async fn test_engine_state_clears_completed_instances() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(SimpleSequentialProcess).unwrap(); // Assuming from previous snippet

    let instance = runtime.run(SimpleSequentialProcess, 10).unwrap();
    let _ = runtime
        .wait_for_completion(&SimpleSequentialProcess, instance)
        .await
        .unwrap()
        .unwrap();

    // Engine should ideally not hang or crash if we poll a deeply completed instance again,
    // or it should return a deterministic "Already Completed" or `None` state depending on storage.
    let duplicate_poll = tokio::time::timeout(
        Duration::from_millis(10),
        runtime.wait_for_completion(&SimpleSequentialProcess, instance),
    )
    .await;

    // The behavior here depends on your InMemory implementation.
    // Generally, querying a completed instance immediately resolves with the final token.
    assert!(
        duplicate_poll.is_ok(),
        "Completed process tokens should be immediately retrievable from storage without hanging."
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_throw_message_ends_cleanly_after_emission() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime.register_process(ThrowMessageProcess).unwrap();
    // Intentionally NOT registering the Wait process.

    let key = CorrelationKey::new();
    let thrower = runtime.run(ThrowMessageProcess, (key, 99)).unwrap();

    // The throw process should reach the end of its sequence flow and terminate gracefully
    // even if no one is listening to the message it threw (fire-and-forget behavior).
    let token = runtime
        .wait_for_completion(&ThrowMessageProcess, thrower)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<i32>(), Some(99));
    assert_eq!(token.last_step().unwrap().as_str(), Step::END);
}
