use std::borrow::Cow;

use chrono::DateTime;

use crate::{
    Process, Storage, Token, Value,
    messages::{CorrelationKey, GuardedCorrelationKey, MessageBroker, MessageMetaData, Messages},
};

/// Allow binding message channels to waitables, which can be used to implement message-based waiting events.
pub trait Bindable {
    /// Optional hook to inject process-scoped message channels.
    fn bind_messages(&mut self, _messages: &MessageBroker) {}
}

/// Asynchronous wait abstraction used for BPMN waiting events.
pub trait Waitable<P: Process, T: Value, O: Value>: Bindable {
    /// Future returned by the wait implementation.
    type Future: Future<Output = O> + Send;

    /// Human readable wait-step name written into token history.
    fn name(&self) -> &str;

    /// Starts waiting for a value and resolves to the produced output.
    fn wait_for<S: Storage>(&self, token: &Token<S>, value: T) -> Self::Future;
}

/// Waitable that resumes when a timer expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timer(pub Cow<'static, str>);

impl Bindable for Timer {}

impl<P: Process> Waitable<P, DateTime<chrono::Utc>, ()> for Timer {
    type Future = futures::future::Either<futures::future::Ready<()>, tokio::time::Sleep>;

    fn name(&self) -> &str {
        &self.0
    }

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
);

impl<P: Process, E: Value> IncomingMessage<P, E> {
    /// Creates a new incoming-message waitable.
    pub fn new(process: P, name: impl Into<Cow<'static, str>>) -> Self {
        IncomingMessage(process, name.into(), None, std::marker::PhantomData)
    }
}

impl<P: Process, E: Value> Bindable for IncomingMessage<P, E> {
    fn bind_messages(&mut self, messages: &MessageBroker) {
        self.2 = Some(messages.get_messages_for_process(self.0.clone()));
    }
}

impl<P: Process, E: Value> Waitable<P, CorrelationKey, E> for IncomingMessage<P, E> {
    type Future = futures::future::BoxFuture<'static, E>;

    fn name(&self) -> &str {
        &self.1
    }

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

        Box::pin(async move {
            loop {
                match receiver.recv().await {
                    Ok(MessageMetaData {
                        correlation_key: Some(metadata_correlation_key),
                        context: _,
                    }) if metadata_correlation_key == correlation_key => {
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

impl<P: Process, E: Value> Waitable<P, GuardedCorrelationKey, E> for IncomingMessage<P, E> {
    type Future = futures::future::BoxFuture<'static, E>;

    fn name(&self) -> &str {
        &self.1
    }

    fn wait_for<S: Storage>(
        &self,
        _token: &Token<S>,
        guarded_correlation_key: GuardedCorrelationKey,
    ) -> Self::Future {
        let messages = self
            .2
            .clone()
            .expect("incoming message waitaxble must be bound to a runtime");

        let mut receiver = messages.subscribe();
        if let Some(message) = messages.receive::<E>(guarded_correlation_key.key) {
            return Box::pin(async move { message });
        }

        Box::pin(async move {
            loop {
                match receiver.recv().await {
                    Ok(MessageMetaData {
                        correlation_key: Some(metadata_correlation_key),
                        context,
                    }) if metadata_correlation_key == guarded_correlation_key.key
                        && context.is_suitable_for(&guarded_correlation_key.expected_sender) =>
                    {
                        if let Some(message) = messages.receive::<E>(metadata_correlation_key) {
                            return message;
                        }
                    }
                    Ok(MessageMetaData {
                        correlation_key: Some(metadata_correlation_key),
                        context: _,
                    }) if metadata_correlation_key == guarded_correlation_key.key => {
                        // Message with matching key but unsuitable sender, ignore.
                    }
                    Ok(_) => {
                        // Message not relevant for this waitable, ignore.
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        panic!("message channel closed before expected message arrived")
                    }
                }
            }
        })
    }
}
