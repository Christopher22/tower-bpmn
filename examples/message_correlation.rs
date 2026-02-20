use axum_bpmn::{
    CorrelationKey, IncomingMessage, Message, Process, ProcessBuilder, Runtime, Token,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MessageTarget;

impl Process for MessageTarget {
    type Input = i32;
    type Output = i32;

    fn name(&self) -> &str {
        "message-target-example"
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
        "wait-message-example"
    }

    fn define(
        &self,
        process: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        process
            .wait_for(IncomingMessage::<MessageTarget, i32>::new(
                MessageTarget,
                "catch-message",
            ))
            .then("to-business-value", |_token: &Token, value| value * 2)
    }
}

#[tokio::main]
async fn main() {
    let mut runtime = Runtime::new(axum_bpmn::executor::TokioExecutor);
    runtime
        .register_process(WaitForMessage)
        .expect("register process");

    let correlation_key = CorrelationKey::new();
    let waiting_instance = runtime
        .run(WaitForMessage, correlation_key)
        .expect("start waiting process");

    runtime
        .send_message(Message {
            process: MessageTarget,
            payload: 21,
            correlation_key,
        })
        .expect("send correlated message");

    let token = waiting_instance.wait_for_completion().await;
    println!("Message catch output: {:?}", token.get_last::<i32>());
    println!("Current task: {}", token.current_task());
}
