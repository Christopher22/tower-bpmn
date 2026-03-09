use schemars::{JsonSchema, json_schema};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    ExtendedExecutor, RegisteredProcess, SharedHistory, State, Token, Value, executor::Executor,
};

/// The ID of a BPMN process instance.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct InstanceId(Uuid);

impl InstanceId {
    /// Create a new instance ID.
    fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for InstanceId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// A instance of a process running in the background.
#[derive(Serialize)]
pub struct Instance<E: ExtendedExecutor> {
    /// The unique identifier of this process instance.
    pub id: InstanceId,
    /// The current status of this process instance.
    pub status: InstanceStatus<E>,
    #[serde(skip)]
    #[allow(dead_code)]
    history: SharedHistory,
}

impl<E: ExtendedExecutor> Instance<E> {
    pub(crate) fn new<V: Value>(process: &RegisteredProcess<E>, executor: E, input: V) -> Self {
        let id = InstanceId::new();
        let history = SharedHistory::new();
        let handle = Handle::new(executor.clone(), process, history.clone(), input);
        Self {
            id,
            status: InstanceStatus::Running(handle),
            history,
        }
    }

    /// Wait for the process instance to complete and return the final context. The context can be used to query the final state of the process.
    pub async fn wait_for_completion(&mut self) -> Result<Token, InstanceNotRunning> {
        self.status.wait_for_completion().await
    }

    /// Stop the process instance and return the context for resuming later.
    pub async fn stop(&mut self) -> Result<(), InstanceNotRunning> {
        self.status.stop().await
    }

    /// Returns the places where this instance currently has token branches.
    pub fn current_places(&self) -> Vec<String> {
        self.history.current_places()
    }
}

impl<E: ExtendedExecutor> std::fmt::Debug for Instance<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Instance")
            .field("uuid", &self.id)
            .finish_non_exhaustive()
    }
}

impl<E: ExtendedExecutor> JsonSchema for Instance<E> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Instance".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        // Include the module, in case a type with the same name is in another module/crate
        concat!(module_path!(), "::Instance").into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        json_schema!(
            {
                "type": "object",
                "properties": {
                    "id": generator.subschema_for::<InstanceId>(),
                    "status": generator.subschema_for::<InstanceStatus<E>>(),
                },
                "required": ["id", "status"],
            }
        )
    }
}

/// The "raw" handle of a process instance.
pub struct Handle<E: ExtendedExecutor> {
    executor: E,
    /// The mutex is only required to guarantee the Handle is Sync.
    task: std::sync::Mutex<<E as Executor<crate::petri_net::Marking<State>>>::TaskHandle>,
    end: crate::petri_net::Id<crate::petri_net::Place<State>>,
}

impl<E: ExtendedExecutor> Handle<E> {
    fn new<V: Value>(
        executor: E,
        process: &RegisteredProcess<E>,
        history: SharedHistory,
        input: V,
    ) -> Self {
        let simulation = process.instantiate(executor.clone(), input, history);
        let task = executor.spawn_task(simulation.run());
        Self {
            executor,
            task: std::sync::Mutex::new(task),
            end: process.end,
        }
    }
}

impl<E: ExtendedExecutor> std::fmt::Debug for Handle<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle").finish_non_exhaustive()
    }
}

#[derive(Debug, Serialize)]
/// Current status of a process.
pub enum InstanceStatus<E: ExtendedExecutor> {
    /// Process is running
    Running(#[serde(skip)] Handle<E>),
    /// Process has completed.
    Completed,
    /// Process has been stopped and can be resumed later.
    Stopped,
}

impl<E: ExtendedExecutor> JsonSchema for InstanceStatus<E> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "InstanceStatus".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        // Include the module, in case a type with the same name is in another module/crate
        concat!(module_path!(), "::InstanceStatus").into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        json_schema!(
            {
                "type": "string",
                "enum": ["running", "completed", "stopped"],
            }
        )
    }
}

impl<E: ExtendedExecutor> InstanceStatus<E> {
    async fn wait_for_completion(&mut self) -> Result<Token, InstanceNotRunning> {
        match std::mem::replace(self, InstanceStatus::Stopped) {
            InstanceStatus::Running(handle) => {
                let mut marking: crate::petri_net::Marking<State> =
                    <E as Executor<crate::petri_net::Marking<State>>>::join(
                        &handle.executor,
                        handle.task.into_inner().unwrap(),
                    )
                    .await
                    .expect("process simulation failed to complete");
                match std::mem::take(&mut marking[handle.end]) {
                    State::Completed(token) => Ok(token),
                    _ => panic!("process completed without token at the end place"),
                }
            }
            _ => Err(InstanceNotRunning),
        }
    }

    async fn stop(&mut self) -> Result<(), InstanceNotRunning> {
        match std::mem::replace(self, InstanceStatus::Stopped) {
            InstanceStatus::Running(handle) => {
                <E as Executor<crate::petri_net::Marking<State>>>::stop(
                    &handle.executor,
                    handle.task.into_inner().unwrap(),
                )
                .await;
                Ok(())
            }
            _ => Err(InstanceNotRunning),
        }
    }
}

impl<E: ExtendedExecutor> std::fmt::Display for InstanceStatus<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstanceStatus::Running(_) => write!(f, "running"),
            InstanceStatus::Completed => write!(f, "completed"),
            InstanceStatus::Stopped => write!(f, "stopped"),
        }
    }
}

/// The instance is not running, either because it has completed or it has been stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InstanceNotRunning;

impl std::fmt::Display for InstanceNotRunning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "instance is not running")
    }
}

impl std::error::Error for InstanceNotRunning {}
