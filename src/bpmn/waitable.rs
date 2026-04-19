use std::borrow::Cow;

use chrono::DateTime;

use crate::bpmn::{
    ExternalStepData, Process, Storage, Token, Value,
    messages::{CorrelationKey, MessageBroker, MessageMetaData, Messages, Participant},
    steps::{Step, StepsBuilder},
    storage::NoOutput,
};

pub(crate) mod internal {
    use crate::bpmn::{
        messages::MessageBroker,
        steps::{Step, StepsBuilder},
    };

    /// Allow binding message channels to waitables, which can be used to implement message-based waiting events.
    pub(crate) trait Registerable {
        /// Optional hook to inject process-scoped message channels.
        fn register(&mut self, steps: &mut StepsBuilder, _messages: &MessageBroker) -> Step;
    }
}

/// Asynchronous wait abstraction used for BPMN waiting events.
pub trait Waitable<P: Process, T: Value, O: Value>: internal::Registerable {
    /// Future returned by the wait implementation.
    type Future: Future<Output = O> + Send;

    /// Starts waiting for a value and resolves to the produced output.
    fn wait_for<S: Storage>(&self, token: &Token<S>, value: T) -> Self::Future;
}

/// Waitable that resumes when a timer expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timer(pub Cow<'static, str>);

impl internal::Registerable for Timer {
    fn register(&mut self, steps: &mut StepsBuilder, _messages: &MessageBroker) -> Step {
        steps.add::<NoOutput>(self.0.clone()).expect("invalid name")
    }
}

impl<P: Process> Waitable<P, DateTime<chrono::Utc>, NoOutput> for Timer {
    type Future = futures::future::Either<futures::future::Ready<NoOutput>, tokio::time::Sleep>;

    fn wait_for<S: Storage>(
        &self,
        _token: &Token<S>,
        value: DateTime<chrono::Utc>,
    ) -> Self::Future {
        chrono::Utc::now()
            .signed_duration_since(value)
            .to_std()
            .map_or_else(
                |_| {
                    // If the value is in the past, return immediately.
                    futures::future::Either::Left(futures::future::ready(()))
                },
                |duration| {
                    // If the value is in the future, wait until then.
                    futures::future::Either::Right(tokio::time::sleep(duration))
                },
            )
    }
}

/// Waitable that resumes when a correlated message arrives.
#[derive(Debug)]
pub struct IncomingMessage<P: Process, E: Value>(
    P,
    Cow<'static, str>,
    Option<Messages>,
    std::marker::PhantomData<fn() -> E>,
    Participant,
);

impl<P: Process, E: Value> IncomingMessage<P, E> {
    /// Creates a new incoming-message waitable.
    pub fn new(process: P, name: impl Into<Cow<'static, str>>) -> Self {
        IncomingMessage(
            process,
            name.into(),
            None,
            std::marker::PhantomData,
            Participant::Everyone,
        )
    }

    /// Limit the senders of messages.
    pub fn with_guard(mut self, expected_sender: Participant) -> Self {
        self.4 = expected_sender;
        self
    }
}

impl<P: Process, E: Value> internal::Registerable for IncomingMessage<P, E> {
    fn register(&mut self, steps: &mut StepsBuilder, messages: &MessageBroker) -> Step {
        let step = steps
            .add_external::<E>(ExternalStepData::new::<E>(self.1.clone(), self.4.clone()))
            .expect("invalid step name");

        let messages = messages.get_messages_for_process(self.0.clone());
        messages.register_input_for_step::<E>(step.clone());
        self.2 = Some(messages);
        step.into()
    }
}

impl<P: Process, E: Value> Waitable<P, CorrelationKey, E> for IncomingMessage<P, E> {
    type Future = futures::future::BoxFuture<'static, E>;

    fn wait_for<S: Storage>(
        &self,
        _token: &Token<S>,
        correlation_key: CorrelationKey,
    ) -> Self::Future {
        let messages = self
            .2
            .clone()
            .expect("incoming message waitable must be bound to a runtime");

        let mut receiver = messages.subscribe();
        if let Some(message) = messages.receive::<E>(correlation_key) {
            return Box::pin(async move { message });
        }

        let expected_sender = self.4.clone();
        Box::pin(async move {
            loop {
                match receiver.recv().await {
                    Ok(MessageMetaData {
                        correlation_key: metadata_correlation_key,
                        context,
                    }) if metadata_correlation_key == correlation_key => {
                        // Check if the message context is suitable for this waitable, which allows implementing guards on the correlation key.
                        if !context.is_suitable_for(&expected_sender) {
                            continue;
                        }
                        if let Some(message) = messages.receive::<E>(correlation_key) {
                            return message;
                        }
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        panic!("message channel closed before expected message arrived")
                    }
                }
            }
        })
    }
}
