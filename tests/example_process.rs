use axum_bpmn::{
    CorrelationKey, IncomingMessage, Message, MetaData, Process, ProcessBuilder, Runtime, Token,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExampleProcess;

impl Process for ExampleProcess {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new(
            "example-process",
            "An example process that demonstrates basic BPMN features.",
        );
        &META
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        let [large, small] = process
            .then("double", |_token: &Token, value: i32| value * 2)
            .split(axum_bpmn::gateways::Xor::for_splitting(
                |_token: &Token, value: i32| {
                    if value >= 10 { 1 } else { 0 }
                },
            ));

        ProcessBuilder::join(
            axum_bpmn::gateways::Xor::for_joining(),
            [
                large.then("large-branch", |_token, value| value + 1),
                small.then("small-branch", |_token, value| value - 1),
            ],
        )
        .then("finalize", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParallelProcess;

impl Process for ParallelProcess {
    type Input = i32;
    type Output = [i32; 2];

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new(
            "parallel-process",
            "A process that demonstrates parallel execution.",
        );
        &META
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        let [left, right] = process.split(axum_bpmn::gateways::And);
        ProcessBuilder::join(
            axum_bpmn::gateways::And,
            [
                left.then("left", |_token, value| value + 1),
                right.then("right", |_token, value| value + 2),
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MessageTarget;

impl Process for MessageTarget {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new(
            "message-target",
            "A process that demonstrates message targeting.",
        );
        &META
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        process.then("identity", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitForMessageProcess;

impl Process for WaitForMessageProcess {
    type Input = CorrelationKey;
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new(
            "wait-for-message",
            "A process that demonstrates waiting for messages.",
        );
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

#[tokio::test(flavor = "current_thread")]
async fn example_process_runs() {
    let mut runtime = Runtime::new(axum_bpmn::executor::TokioExecutor);
    runtime
        .register_process(ExampleProcess)
        .expect("process registration must work");

    let token = runtime
        .run(ExampleProcess, 6)
        .expect("instance must start")
        .wait_for_completion()
        .await;

    assert_eq!(token.get_last::<i32>(), Some(11));
    assert_eq!(token.last_step(), Some("finalize".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn and_join_combines_parallel_outputs() {
    let mut runtime = Runtime::new(axum_bpmn::executor::TokioExecutor);
    runtime
        .register_process(ParallelProcess)
        .expect("process registration must work");

    let token = runtime
        .run(ParallelProcess, 10)
        .expect("instance must start")
        .wait_for_completion()
        .await;

    assert_eq!(token.get_last::<[i32; 2]>(), Some([11, 12]));
}

#[tokio::test(flavor = "current_thread")]
async fn incoming_message_unblocks_wait_step() {
    let mut runtime = Runtime::new(axum_bpmn::executor::TokioExecutor);
    runtime
        .register_process(WaitForMessageProcess)
        .expect("process registration must work");

    let correlation_key = CorrelationKey::new();
    let waiter = runtime
        .run(WaitForMessageProcess, correlation_key)
        .expect("instance must start");

    runtime
        .send_message(Message {
            process: MessageTarget,
            payload: 21,
            correlation_key,
        })
        .expect("sending message must work");

    let token = waiter.wait_for_completion().await;
    assert_eq!(token.get_last::<i32>(), Some(42));
}
