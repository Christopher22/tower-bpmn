use axum_bpmn::{
    CorrelationKey, InMemory, IncomingMessage, Message, Process, ProcessBuilder, Runtime, Storage,
    Token,
};

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
                |_token: &Token<S>, (correlation_key, payload): (CorrelationKey, i32)| Message {
                    process: MessageTarget,
                    payload,
                    correlation_key,
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
        let [left, right] = process.split(axum_bpmn::gateways::And);
        ProcessBuilder::join(
            axum_bpmn::gateways::And,
            [
                left.then("left-path", |_token, value| value + 10),
                right.then("right-path", |_token, value| value + 20),
            ],
        )
    }
}

#[tokio::test(flavor = "current_thread")]
async fn throw_then_catch_message_event_with_correlation() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime
        .register_process(ThrowMessageProcess)
        .expect("throw process registration must succeed");
    runtime
        .register_process(WaitForMessageProcess)
        .expect("wait process registration must succeed");

    let key = CorrelationKey::new();
    let thrower = runtime
        .run(ThrowMessageProcess, (key, 14))
        .expect("thrower instance must start");
    let waiter = runtime
        .run(WaitForMessageProcess, key)
        .expect("waiter instance must start");

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
async fn correlation_keys_isolate_parallel_message_instances() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime
        .register_process(WaitForMessageProcess)
        .expect("wait process registration must succeed");

    let key_a = CorrelationKey::new();
    let key_b = CorrelationKey::new();

    let waiter_a = runtime
        .run(WaitForMessageProcess, key_a)
        .expect("first waiter instance must start");
    let waiter_b = runtime
        .run(WaitForMessageProcess, key_b)
        .expect("second waiter instance must start");

    runtime
        .send_message(Message {
            process: MessageTarget,
            payload: 7,
            correlation_key: key_b,
        })
        .expect("message for key_b must be accepted");
    runtime
        .send_message(Message {
            process: MessageTarget,
            payload: 5,
            correlation_key: key_a,
        })
        .expect("message for key_a must be accepted");

    let (token_a, token_b) = tokio::join!(
        runtime.wait_for_completion(&WaitForMessageProcess, waiter_a),
        runtime.wait_for_completion(&WaitForMessageProcess, waiter_b)
    );

    assert_eq!(token_a.unwrap().unwrap().get_last::<i32>(), Some(15));
    assert_eq!(token_b.unwrap().unwrap().get_last::<i32>(), Some(21));
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_gateway_join_produces_combined_data_object() {
    let mut runtime: Runtime<axum_bpmn::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime
        .register_process(ParallelAggregationProcess)
        .expect("parallel process registration must succeed");

    let instance = runtime
        .run(ParallelAggregationProcess, 3)
        .expect("parallel process instance must start");
    let token = runtime
        .wait_for_completion(&ParallelAggregationProcess, instance)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(token.get_last::<[i32; 2]>(), Some([13, 23]));
    assert_eq!(token.last_step(), Some("AND Join".to_string()));
}
