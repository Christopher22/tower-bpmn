use schemars::{JsonSchema, json_schema};
use serde::{Serialize, ser::SerializeStruct};
use uuid::Uuid;

use crate::{
    ExtendedExecutor, RegisteredProcess, State, Storage, StorageBackend, Token, Value,
    executor::Executor,
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
pub struct Instance<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    /// The unique identifier of this process instance.
    pub id: InstanceId,
    /// The current status of this process instance.
    pub status: InstanceStatus<E, B>,
    history: B::Storage,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Instance<E, B> {
    pub(crate) fn new<V: Value>(
        process: &RegisteredProcess<E, B>,
        storage_backend: &B,
        executor: E,
        input: V,
    ) -> Self {
        let id = InstanceId::new();
        let storage = storage_backend.new_instance(id);
        let handle = Handle::new(executor.clone(), process, storage.clone(), input);
        Self {
            id,
            status: InstanceStatus::Running(handle),
            history: storage,
        }
    }

    /// Wait for the process instance to complete and return the final context. The context can be used to query the final state of the process.
    pub async fn wait_for_completion(&mut self) -> Result<Token<B::Storage>, InstanceNotRunning> {
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

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> std::fmt::Debug for Instance<E, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Instance")
            .field("uuid", &self.id)
            .finish_non_exhaustive()
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Serialize for Instance<E, B> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut struct_serializer = serializer.serialize_struct("Instance", 2)?;
        struct_serializer.serialize_field("id", &self.id)?;
        struct_serializer.serialize_field("status", &self.status)?;
        struct_serializer.end()
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> JsonSchema for Instance<E, B> {
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
                    "status": generator.subschema_for::<InstanceStatus<E, B>>(),
                },
                "required": ["id", "status"],
            }
        )
    }
}

/// The "raw" handle of a process instance.
pub struct Handle<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    executor: E,
    /// The mutex is only required to guarantee the Handle is Sync.
    task:
        std::sync::Mutex<<E as Executor<crate::petri_net::Marking<State<B::Storage>>>>::TaskHandle>,
    end: crate::petri_net::Id<crate::petri_net::Place<State<B::Storage>>>,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Handle<E, B> {
    fn new<V: Value>(
        executor: E,
        process: &RegisteredProcess<E, B>,
        storage: B::Storage,
        input: V,
    ) -> Self {
        let simulation = process.instantiate(executor.clone(), input, storage);
        let task = executor.spawn_task(simulation.run());
        Self {
            executor,
            task: std::sync::Mutex::new(task),
            end: process.end,
        }
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> std::fmt::Debug for Handle<E, B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle").finish_non_exhaustive()
    }
}

#[derive(Debug, Serialize)]
/// Current status of a process.
pub enum InstanceStatus<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    /// Process is running
    Running(#[serde(skip)] Handle<E, B>),
    /// Process has completed.
    Completed,
    /// Process has been stopped and can be resumed later.
    Stopped,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> JsonSchema for InstanceStatus<E, B> {
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

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> InstanceStatus<E, B> {
    async fn wait_for_completion(&mut self) -> Result<Token<B::Storage>, InstanceNotRunning> {
        match std::mem::replace(self, InstanceStatus::Stopped) {
            InstanceStatus::Running(handle) => {
                let mut marking: crate::petri_net::Marking<State<B::Storage>> =
                    <E as Executor<crate::petri_net::Marking<State<B::Storage>>>>::join(
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
                <E as Executor<crate::petri_net::Marking<State<B::Storage>>>>::stop(
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

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> std::fmt::Display
    for InstanceStatus<E, B>
{
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
