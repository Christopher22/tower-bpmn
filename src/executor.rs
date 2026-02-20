/// A backend executor for running tasks.
pub trait Executor<T: 'static + Send>: Clone {
    /// The handle type for spawned tasks.
    type TaskHandle: Send;

    /// Spawn a new asynchronous task.
    fn spawn_task(
        &self,
        value: impl std::future::Future<Output = T> + 'static + Send,
    ) -> Self::TaskHandle;

    /// Stop a running task.
    fn stop(&self, task: Self::TaskHandle) -> impl std::future::Future<Output = ()>;
}

impl Executor<()> for futures::executor::LocalSpawner {
    type TaskHandle = ();

    fn spawn_task(
        &self,
        value: impl std::future::Future<Output = ()> + 'static + Send,
    ) -> Self::TaskHandle {
        use futures::{FutureExt, task::Spawn};
        self.spawn_obj(value.boxed().into()).unwrap();
    }

    async fn stop(&self, _task: Self::TaskHandle) {}
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokioExecutor;
impl<T: 'static + Send> Executor<T> for TokioExecutor {
    type TaskHandle = tokio::task::JoinHandle<T>;

    fn spawn_task(
        &self,
        value: impl std::future::Future<Output = T> + 'static + Send,
    ) -> Self::TaskHandle {
        tokio::task::spawn(value)
    }

    async fn stop(&self, task: Self::TaskHandle) {
        task.abort();
        let _ = task.await;
    }
}
