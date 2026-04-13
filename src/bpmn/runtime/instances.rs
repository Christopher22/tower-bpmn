use dashmap::DashMap;
use serde::ser::{SerializeSeq, SerializeStruct};

use super::super::ExtendedExecutor;
use super::{Instance, InstanceId, RegisteredProcess, StorageError, StorageBackend, Value};

/// A collection of process instances for a specific registered process definition.
#[derive(Debug)]
pub struct Instances<E: ExtendedExecutor<B::Storage>, B: StorageBackend> {
    /// The registered process definition for this set of instances.
    pub registered_process: RegisteredProcess<B>,
    instances: DashMap<InstanceId, Instance<E, B>>,
    executor: E,
    storage_backend: B,
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> Instances<E, B> {
    /// Create a new instance object.
    pub fn new(registered_process: RegisteredProcess<B>, executor: E, storage_backend: B) -> Self {
        Instances {
            registered_process,
            instances: DashMap::new(),
            executor,
            storage_backend,
        }
    }

    /// Returns an iterator over tracked process instances.
    pub fn iter(&self) -> impl Iterator<Item = impl std::ops::Deref<Target = Instance<E, B>>> {
        self.instances.iter()
    }

    /// Returns one tracked instance by id.
    pub fn get(&self, id: InstanceId) -> Option<impl std::ops::Deref<Target = Instance<E, B>>> {
        self.instances.get(&id)
    }

    /// Try to wait for a specific instance to complete and return the final context. Returns None if the instance is not found, or Some(Err) if the instance is not running.
    pub async fn wait_for_completion(
        &self,
        id: InstanceId,
    ) -> Option<Result<super::Token<B::Storage>, super::InstanceNotRunning>> {
        match self.instances.get_mut(&id) {
            Some(mut instance) => Some(instance.wait_for_completion().await),
            None => None,
        }
    }

    /// Run a new instance. Used internally by the runtime.
    /// This should not be called directly, because the value is not checked and will panic if not match the registered process.
    pub(crate) fn run<V: Value>(&self, input: V) -> InstanceId {
        let instance = Instance::new(
            &self.registered_process,
            &self.storage_backend,
            self.executor.clone(),
            input,
        );
        let id = instance.id;
        self.instances.insert(id, instance);
        id
    }

    /// Try to resume a paused instance.
    pub fn resume(&self, id: InstanceId) -> Result<InstanceId, StorageError> {
        let instance = Instance::resume(
            &self.registered_process,
            self.executor.clone(),
            self.storage_backend
                .resume_instance(&self.registered_process, id)?,
        );
        let id = instance.id;
        self.instances.insert(id, instance);
        Ok(id)
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> serde::Serialize for Instances<E, B> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serializer = serializer.serialize_struct("Instances", 1)?;
        struct InstanceList<'a, E: ExtendedExecutor<B::Storage>, B: StorageBackend>(
            &'a DashMap<InstanceId, Instance<E, B>>,
        );

        impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> serde::Serialize
            for InstanceList<'_, E, B>
        {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
                for instance in self.0.iter() {
                    sequence.serialize_element(instance.value())?;
                }
                sequence.end()
            }
        }

        serializer.serialize_field("instances", &InstanceList(&self.instances))?;
        serializer.end()
    }
}

impl<E: ExtendedExecutor<B::Storage>, B: StorageBackend> schemars::JsonSchema for Instances<E, B> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Instances".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Instance").into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "object",
            "properties": {
                "instances": {
                    "type": "array",
                    "items": generator.subschema_for::<Instance<E, B>>()
                }
            },
            "required": ["instances"],
        })
    }
}

/// Errors while creating or resolving an instance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InstanceSpawnError {
    /// Not registered
    Unregistered,
    /// Given the context, the process instance has already completed.
    Completed,
    /// The context does not match the process instance.
    InvalidContext,
    /// The input value is invalid for the process instance. This could only be emitted by the dynamic API, because the static API checks the input type at compile time.
    InvalidInput(String),
}
