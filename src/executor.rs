use std::future::Future;

use schemars::JsonSchema;
use serde::Serialize;

/// A backend executor for running tasks.
pub trait Executor<T: 'static + Send>: Clone {
    /// The handle type for spawned tasks.
    type TaskHandle: Send;

    /// Spawn a new asynchronous task.
    fn spawn_task(&self, value: impl Future<Output = T> + 'static + Send) -> Self::TaskHandle;

    /// Stop a running task.
    fn stop(&self, task: Self::TaskHandle) -> impl Future<Output = ()> + Send;

    /// Wait for a running task to finish and return its output.
    fn join(&self, task: Self::TaskHandle) -> impl Future<Output = Option<T>> + Send;
}

impl Executor<()> for futures::executor::LocalSpawner {
    type TaskHandle = ();

    fn spawn_task(&self, value: impl Future<Output = ()> + 'static + Send) -> Self::TaskHandle {
        use futures::{FutureExt, task::Spawn};
        self.spawn_obj(value.boxed().into()).unwrap();
    }

    fn stop(&self, _task: Self::TaskHandle) -> impl Future<Output = ()> + Send {
        futures::future::ready(())
    }

    fn join(&self, _task: Self::TaskHandle) -> impl Future<Output = Option<()>> + Send {
        futures::future::ready(Some(()))
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, JsonSchema)]
/// Executor implementation backed by `tokio::task::spawn`.
pub struct TokioExecutor;
impl<T: 'static + Send> Executor<T> for TokioExecutor {
    type TaskHandle = tokio::task::JoinHandle<T>;

    fn spawn_task(&self, value: impl Future<Output = T> + 'static + Send) -> Self::TaskHandle {
        tokio::task::spawn(value)
    }

    async fn stop(&self, task: Self::TaskHandle) {
        task.abort();
        let _ = task.await;
    }

    async fn join(&self, task: Self::TaskHandle) -> Option<T> {
        task.await.ok()
    }
}
