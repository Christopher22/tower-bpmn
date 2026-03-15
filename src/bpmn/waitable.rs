use std::borrow::Cow;

use chrono::DateTime;

use crate::{
    Process, Storage, Token, Value,
    messages::{CorrelationKey, Messages},
};

/// Asynchronous wait abstraction used for BPMN waiting events.
pub trait Waitable<P: Process, T: Value, O: Value> {
    /// Future returned by the wait implementation.
    type Future: Future<Output = O> + Send;

    /// Human readable wait-step name written into token history.
    fn name(&self) -> &str;

    /// Optional hook to inject process-scoped message channels.
    fn bind_messages(&mut self, _messages: Messages) {}

    /// Starts waiting for a value and resolves to the produced output.
    fn wait_for<S: Storage>(&self, token: &Token<S>, value: T) -> Self::Future;
}

/// Waitable that resumes when a timer expires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timer(pub Cow<'static, str>);

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

impl<P: Process, E: Value> Waitable<P, CorrelationKey, E> for IncomingMessage<P, E> {
    type Future = futures::future::BoxFuture<'static, E>;

    fn name(&self) -> &str {
        &self.1
    }

    fn bind_messages(&mut self, messages: Messages) {
        self.2 = Some(messages);
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
                    Ok(key) if key == correlation_key => {
                        if let Some(message) = messages.receive::<E>(key) {
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
