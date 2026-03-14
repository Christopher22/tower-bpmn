use chrono::{DateTime, Duration, Utc};
use tokio::time::{Duration as TokioDuration, timeout};

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DummyProcess;

impl Process for DummyProcess {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new("dummy", "A dummy process for testing.");
        &META
    }

    fn define<S: Storage>(
        &self,
        builder: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        builder.then("identity", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct XorUnitProcess;

impl Process for XorUnitProcess {
    type Input = i32;
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new(
            "xor-unit",
            "A process that demonstrates XOR splitting and joining.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        builder: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        let [left, right] =
            builder
                .then("prepare", |_token, value| value)
                .split(gateways::Xor::for_splitting(
                    "Modulo",
                    |_token, value: i32| {
                        if value % 2 == 0 { 0 } else { 1 }
                    },
                ));

        ProcessBuilder::join(
            gateways::Xor::for_joining("Post modulo"),
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

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new(
            "message-target-unit",
            "A process that demonstrates message targeting.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        builder: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        builder.then("identity", |_token, value| value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ThrowingProcess;

impl Process for ThrowingProcess {
    type Input = (CorrelationKey, i32);
    type Output = i32;

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new(
            "throwing-unit",
            "A process that demonstrates throwing messages.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        builder: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
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

    fn metadata(&self) -> &MetaData {
        static META: MetaData = MetaData::new(
            "waiting-unit",
            "A process that demonstrates waiting for messages.",
        );
        &META
    }

    fn define<S: Storage>(
        &self,
        builder: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S> {
        builder
            .wait_for(IncomingMessage::<MessageTargetProcess, i32>::new(
                MessageTargetProcess,
                "incoming",
            ))
            .then("double", |_token, value| value * 2)
    }
}

#[test]
fn runtime_returns_unregistered_error_for_unknown_process() {
    let runtime: Runtime<crate::executor::TokioExecutor, InMemory> = Runtime::default();
    let result = runtime.run(DummyProcess, 1);
    assert!(matches!(result, Err(InstanceSpawnError::Unregistered)));
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
        &Token::new(InMemoryStorage::for_test()),
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
            &Token::new(InMemoryStorage::for_test()),
            Utc::now() + Duration::milliseconds(30),
        ),
    )
    .await;

    assert!(result.is_ok());
}

#[test]
fn xor_process_definition_registers_successfully() {
    let mut runtime: Runtime<crate::executor::TokioExecutor, InMemory> = Runtime::default();
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

    let value = waitable
        .wait_for(&Token::new(InMemoryStorage::for_test()), key)
        .await;
    assert_eq!(value, 77);
}

#[tokio::test(flavor = "current_thread")]
async fn incoming_message_waitable_ignores_other_correlation_keys() {
    let mut waitable = IncomingMessage::<DummyProcess, i32>::new(DummyProcess, "incoming");
    let messages = ProcessMessages::new();
    waitable.bind_messages(messages.clone());

    let expected_key = CorrelationKey::new();
    let other_key = CorrelationKey::new();

    let wait_future = waitable.wait_for(&Token::new(InMemoryStorage::for_test()), expected_key);
    messages.send(other_key, 11_i32);
    messages.send(expected_key, 88_i32);

    let value = timeout(TokioDuration::from_secs(1), wait_future)
        .await
        .expect("waitable should resolve for the expected key");
    assert_eq!(value, 88);
}

#[tokio::test(flavor = "current_thread")]
async fn throw_message_and_wait_for_message_roundtrip() {
    let mut runtime: Runtime<crate::executor::TokioExecutor, InMemory> = Runtime::default();
    runtime
        .register_process(ThrowingProcess)
        .expect("throw process registration must work");
    runtime
        .register_process(WaitingProcess)
        .expect("wait process registration must work");

    let key = CorrelationKey::new();
    let throw_instance: InstanceId = runtime
        .run(ThrowingProcess, (key, 12))
        .expect("throw instance must start");
    let wait_instance = runtime
        .run(WaitingProcess, key)
        .expect("wait instance must start");

    let throw_token = runtime
        .wait_for_completion(&ThrowingProcess, throw_instance)
        .await
        .expect("process registered")
        .expect("process running");

    let wait_token = runtime
        .wait_for_completion(&WaitingProcess, wait_instance)
        .await
        .expect("process registered")
        .expect("process running");

    assert_eq!(throw_token.get_last::<i32>(), Some(12));
    assert_eq!(wait_token.get_last::<i32>(), Some(24));
}
