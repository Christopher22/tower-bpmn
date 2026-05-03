use std::borrow::Cow;

use chrono::DateTime;
use futures::FutureExt;

use crate::bpmn::{
    ExternalStepData, Extract, Process, Storage, Token, Value,
    messages::{CorrelationKey, Entity, MessageBroker, Messages, Participant},
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
    type Future: Future<Output = (Entity, O)> + Send;

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

fn skip_output(_: ()) -> (Entity, NoOutput) {
    (Entity::SYSTEM, ())
}

impl<P: Process> Waitable<P, DateTime<chrono::Utc>, NoOutput> for Timer {
    type Future = futures::future::Either<
        futures::future::Ready<(Entity, NoOutput)>,
        futures::future::Map<tokio::time::Sleep, fn(()) -> (Entity, NoOutput)>,
    >;

    #[allow(trivial_casts)]
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
                    futures::future::Either::Left(futures::future::ready((Entity::SYSTEM, ())))
                },
                |duration| {
                    // If the value is in the future, wait until then.
                    futures::future::Either::Right(
                        tokio::time::sleep(duration).map(skip_output as fn(()) -> (Entity, ())),
                    )
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

    fn try_receive(
        messages: &Messages,
        correlation_key: CorrelationKey,
        expected_sender: &Participant,
    ) -> Option<(Entity, E)> {
        let (metadata, message) = messages.receive::<E>(correlation_key)?;
        if !metadata.context.is_suitable_for(expected_sender) {
            return None;
        }

        let responsible = metadata.context.responsible_entity()?.clone();
        Some((responsible, message))
    }

    fn wait_for_message(
        &self,
        correlation_key: CorrelationKey,
        expected_sender: Participant,
    ) -> futures::future::BoxFuture<'static, (Entity, E)> {
        let messages = self
            .2
            .clone()
            .expect("incoming message waitable must be bound to a runtime");

        if let Some((responsible, message)) =
            Self::try_receive(&messages, correlation_key, &expected_sender)
        {
            return Box::pin(async move { (responsible, message) });
        }

        let mut receiver = messages.subscribe();
        Box::pin(async move {
            loop {
                if let Some((responsible, message)) =
                    Self::try_receive(&messages, correlation_key, &expected_sender)
                {
                    return (responsible, message);
                }

                match receiver.recv().await {
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
    type Future = futures::future::BoxFuture<'static, (Entity, E)>;

    fn wait_for<S: Storage>(
        &self,
        _token: &Token<S>,
        correlation_key: CorrelationKey,
    ) -> Self::Future {
        let expected_sender = self.4.clone();
        self.wait_for_message(correlation_key, expected_sender)
    }
}

impl<P: Process, E: Value, I: Extract<CorrelationKey> + Extract<Participant>> Waitable<P, I, E>
    for IncomingMessage<P, E>
{
    type Future = futures::future::BoxFuture<'static, (Entity, E)>;

    fn wait_for<S: Storage>(&self, _token: &Token<S>, input: I) -> Self::Future {
        let correlation_key = input.extract();
        let expected_sender = input.extract();
        self.wait_for_message(correlation_key, expected_sender)
    }
}
