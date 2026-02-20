use chrono::{DateTime, Duration, Utc};
use tokio::time::{Duration as TokioDuration, timeout};

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DummyProcess;

impl Process for DummyProcess {
    type Input = i32;
    type Output = i32;

    fn name(&self) -> &str {
        "dummy"
    }

    fn define(
        &self,
        builder: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        builder.then("identity", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct XorUnitProcess;

impl Process for XorUnitProcess {
    type Input = i32;
    type Output = i32;

    fn name(&self) -> &str {
        "xor-unit"
    }

    fn define(
        &self,
        builder: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        let [left, right] =
            builder
                .then("prepare", |_token, value| value)
                .split(gateways::Xor::for_splitting(|_token, value: i32| {
                    if value % 2 == 0 { 0 } else { 1 }
                }));

        ProcessBuilder::join(
            gateways::Xor::for_joining(),
            [
                left.then("left", |_token, value| value + 10),
                right.then("right", |_token, value| value + 100),
            ],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MessageTargetProcess;

impl Process for MessageTargetProcess {
    type Input = i32;
    type Output = i32;

    fn name(&self) -> &str {
        "message-target-unit"
    }

    fn define(
        &self,
        builder: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        builder.then("identity", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ThrowingProcess;

impl Process for ThrowingProcess {
    type Input = (CorrelationKey, i32);
    type Output = i32;

    fn name(&self) -> &str {
        "throwing-unit"
    }

    fn define(
        &self,
        builder: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        builder
            .throw_message("throw", |_token, (key, payload)| Message {
                process: MessageTargetProcess,
                payload,
                correlation_key: key,
            })
            .then("done", |_token, (_key, payload)| payload)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WaitingProcess;

impl Process for WaitingProcess {
    type Input = CorrelationKey;
    type Output = i32;

    fn name(&self) -> &str {
        "waiting-unit"
    }

    fn define(
        &self,
        builder: ProcessBuilder<Self, Self::Input>,
    ) -> ProcessBuilder<Self, Self::Output> {
        builder
            .wait_for(IncomingMessage::<MessageTargetProcess, i32>::new(
                MessageTargetProcess,
                "incoming",
            ))
            .then("double", |_token, value| value * 2)
    }
}

#[test]
fn token_history_returns_latest_value_and_task() {
    let token = Token::new()
        .set_output("step-a", 1_i32)
        .set_output("step-b", 2_i32)
        .set_output("step-c", 3_i32);

    assert_eq!(token.get_last::<i32>(), Some(3));
    assert_eq!(token.current_task(), "step-c".to_string());
}

#[test]
fn token_child_branch_keeps_parent_history_visible() {
    let root = Token::new().set_output("root", 5_i32);
    let child = root.clone().set_output("child", 9_i32);

    assert_eq!(root.get_last::<i32>(), Some(5));
    assert_eq!(child.get_last::<i32>(), Some(9));
}

#[test]
fn runtime_returns_unregistered_error_for_unknown_process() {
    let runtime = Runtime::new(crate::executor::TokioExecutor);
    let result = runtime.run(DummyProcess, 1);
    assert!(matches!(result, Err(InstanceError::Unregistered)));
}

#[test]
fn message_manager_returns_no_target_when_process_not_registered() {
    let manager = MessageManager::new();
    let result = manager.send_message(Message {
        process: MessageTargetProcess,
        payload: 10,
        correlation_key: CorrelationKey::new(),
    });
    assert_eq!(result, Err(SendError::NoTarget));
}

#[tokio::test(flavor = "current_thread")]
async fn timer_waitable_with_past_time_resolves_immediately() {
    let timer = Timer("timer".into());
    let output = <Timer as Waitable<DummyProcess, DateTime<Utc>, ()>>::wait_for(
        &timer,
        &Token::new(),
        Utc::now() - Duration::seconds(1),
    )
    .await;
    let _: () = output;
}

#[tokio::test(flavor = "current_thread")]
async fn timer_waitable_with_future_time_resolves_with_current_semantics() {
    let timer = Timer("timer-future".into());

    let result = timeout(
        TokioDuration::from_millis(50),
        <Timer as Waitable<DummyProcess, DateTime<Utc>, ()>>::wait_for(
            &timer,
            &Token::new(),
            Utc::now() + Duration::milliseconds(30),
        ),
    )
    .await;

    assert!(result.is_ok());
}

#[test]
fn xor_process_definition_registers_successfully() {
    let mut runtime = Runtime::new(crate::executor::TokioExecutor);
    runtime
        .register_process(XorUnitProcess)
        .expect("process registration must work");
}

#[tokio::test(flavor = "current_thread")]
async fn incoming_message_waitable_returns_payload() {
    let mut waitable = IncomingMessage::<DummyProcess, i32>::new(DummyProcess, "incoming");
    let messages = ProcessMessages::new();
    waitable.bind_messages(messages.clone());

    let key = CorrelationKey::new();
    messages.send(key, 77_i32);

    let value = waitable.wait_for(&Token::new(), key).await;
    assert_eq!(value, 77);
}

#[tokio::test(flavor = "current_thread")]
async fn incoming_message_waitable_ignores_other_correlation_keys() {
    let mut waitable = IncomingMessage::<DummyProcess, i32>::new(DummyProcess, "incoming");
    let messages = ProcessMessages::new();
    waitable.bind_messages(messages.clone());

    let expected_key = CorrelationKey::new();
    let other_key = CorrelationKey::new();

    let wait_future = waitable.wait_for(&Token::new(), expected_key);
    messages.send(other_key, 11_i32);
    messages.send(expected_key, 88_i32);

    let value = timeout(TokioDuration::from_secs(1), wait_future)
        .await
        .expect("waitable should resolve for the expected key");
    assert_eq!(value, 88);
}

#[tokio::test(flavor = "current_thread")]
async fn throw_message_and_wait_for_message_roundtrip() {
    let mut runtime = Runtime::new(crate::executor::TokioExecutor);
    runtime
        .register_process(ThrowingProcess)
        .expect("throw process registration must work");
    runtime
        .register_process(WaitingProcess)
        .expect("wait process registration must work");

    let key = CorrelationKey::new();
    let throw_instance = runtime
        .run(ThrowingProcess, (key, 12))
        .expect("throw instance must start");
    let wait_instance = runtime
        .run(WaitingProcess, key)
        .expect("wait instance must start");

    let throw_token = throw_instance.wait_for_completion().await;
    let wait_token = wait_instance.wait_for_completion().await;

    assert_eq!(throw_token.get_last::<i32>(), Some(12));
    assert_eq!(wait_token.get_last::<i32>(), Some(24));
}
