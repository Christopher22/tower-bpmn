use std::{
    any::{Any, TypeId},
    sync::Arc,
};

use chrono::DateTime;
use dashmap::DashMap;
use serde_json::Value as JsonValue;

use super::{
    InstanceId, ProcessName, RegisteredProcess, ResumableProcess, Step, Storage, StorageBackend,
    StorageError, TokenId, Value,
};

#[derive(Debug)]
struct Entry {
    timestamp: DateTime<chrono::Utc>,
    place: Step,
    token_id: TokenId,
    /// This is enforced to be a "Value"
    value: Box<dyn Any + Send + Sync>,
    serialize_json: fn(&(dyn Any + Send + Sync)) -> JsonValue,
}

impl Entry {
    fn serialize_json_impl<V: Value>(value: &(dyn Any + Send + Sync)) -> JsonValue {
        let typed = value.downcast_ref::<V>().expect("checked type");
        serde_json::to_value(typed).expect("value must serialize")
    }

    pub fn new<V: Value>(token_id: TokenId, place: Step, value: V) -> Self {
        Entry {
            timestamp: chrono::Utc::now(),
            place,
            token_id,
            value: Box::new(value),
            serialize_json: Self::serialize_json_impl::<V>,
        }
    }
}

#[derive(Debug)]
struct InstanceState {
    process_name: ProcessName,
    history: Arc<RawHistory>,
}

#[derive(Debug)]
struct RawHistory {
    entries: DashMap<TypeId, Vec<Entry>>,
    current_places: DashMap<TokenId, Step>,
}

/// An in-memory storage backend, which is suitable for testing and simple use cases.
#[derive(Debug, Clone)]
pub struct InMemory(Arc<DashMap<InstanceId, Arc<InstanceState>>>);

impl Default for InMemory {
    fn default() -> Self {
        InMemory(Arc::new(DashMap::new()))
    }
}

impl StorageBackend for InMemory {
    type Storage = InMemoryStorage;

    fn query(
        &self,
        process: &RegisteredProcess<Self>,
        step: Step,
        instance_id: InstanceId,
    ) -> Result<Vec<JsonValue>, StorageError> {
        let Some(instance) = self.0.get(&instance_id) else {
            return Err(StorageError::NotFound);
        };

        let expected_process = ProcessName::from(&process.meta_data);
        if instance.process_name != expected_process {
            return Err(StorageError::ProcessMismatch);
        }

        let mut rows = instance
            .history
            .entries
            .iter()
            .flat_map(|typed_entries| {
                typed_entries
                    .value()
                    .iter()
                    .filter(|entry| entry.place == step)
                    .map(|entry| {
                        (
                            entry.timestamp,
                            (entry.serialize_json)(entry.value.as_ref()),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        rows.sort_by_key(|(timestamp, _)| *timestamp);

        Ok(rows.into_iter().map(|(_, value)| value).collect())
    }

    fn new_instance(
        &self,
        process: &RegisteredProcess<Self>,
        process_id: InstanceId,
    ) -> Self::Storage {
        let history = Arc::new(RawHistory {
            entries: DashMap::new(),
            current_places: DashMap::new(),
        });
        self.0.insert(
            process_id,
            Arc::new(InstanceState {
                process_name: ProcessName::from(&process.meta_data),
                history: history.clone(),
            }),
        );
        InMemoryStorage(history)
    }

    fn resume_instance(
        &self,
        _process: &RegisteredProcess<Self>,
        _process_id: InstanceId,
    ) -> Result<ResumableProcess<Self>, StorageError> {
        Err(StorageError::NotFound)
    }

    fn unfinished_instances(&self) -> Vec<(ProcessName, InstanceId)> {
        Vec::new()
    }
}

/// A (shared) history of token values, indexed by type and token id, with timestamps for determining the current value at a given place in the process. This is shared between all tokens in the same process instance, allowing them to see each other's values according to their branching history.
#[derive(Clone)]
pub struct InMemoryStorage(Arc<RawHistory>);

impl InMemoryStorage {
    #[cfg(test)]
    /// Creates an empty history for testing purposes.
    pub fn for_test() -> Self {
        InMemoryStorage(Arc::new(RawHistory {
            entries: DashMap::new(),
            current_places: DashMap::new(),
        }))
    }
}

impl PartialEq for InMemoryStorage {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for InMemoryStorage {}

impl std::fmt::Debug for InMemoryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedHistory").finish_non_exhaustive()
    }
}

impl Storage for InMemoryStorage {
    fn add<V: Value>(&self, token_id: TokenId, place: Step, value: V) {
        let data_entry = Entry::new(token_id, place.clone(), value);
        match self.0.entries.entry(TypeId::of::<V>()) {
            dashmap::Entry::Occupied(mut entry) => {
                entry.get_mut().push(data_entry);
            }
            dashmap::Entry::Vacant(entry) => {
                entry.insert(vec![data_entry]);
            }
        }
        self.0.current_places.insert(token_id, place);
    }

    fn current_places(&self) -> Vec<Step> {
        self.0
            .current_places
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    fn get_last<T: Value>(&self, token_ids: &[TokenId]) -> Option<T> {
        self.0.entries.get(&TypeId::of::<T>()).and_then(|entries| {
            entries
                .iter()
                .rev()
                .find(|entry| token_ids.contains(&entry.token_id))
                .map(|entry| {
                    entry
                        .value
                        .downcast_ref::<T>()
                        .expect("checked type")
                        .clone()
                })
        })
    }

    /// Returns the name of the last step finished by this token.
    fn last_step(&self, token_ids: &[TokenId]) -> Option<Step> {
        self.0
            .entries
            .iter()
            .flat_map(|typed_entries| {
                typed_entries
                    .value()
                    .iter()
                    .map(|entry| (entry.timestamp, entry.place.clone(), entry.token_id))
                    .collect::<Vec<_>>()
            })
            .filter(|(_, _, token_id)| token_ids.contains(token_id))
            .max_by_key(|(timestamp, _, _)| *timestamp)
            .map(|(_, place, _)| place)
    }
}

#[cfg(test)]
mod tests {
    use crate::bpmn::{MetaData, Process, ProcessBuilder, Runtime, Token};
    use crate::executor::TokioExecutor;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct InMemoryQueryProcess;

    impl Process for InMemoryQueryProcess {
        type Input = i32;
        type Output = i32;

        fn metadata(&self) -> &MetaData {
            static META: MetaData =
                MetaData::new("in-memory-query", "in-memory query backend tests");
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
    struct InMemoryOtherProcess;

    impl Process for InMemoryOtherProcess {
        type Input = i32;
        type Output = i32;

        fn metadata(&self) -> &MetaData {
            static META: MetaData =
                MetaData::new("in-memory-query-other", "in-memory query mismatch tests");
            &META
        }

        fn define<S: Storage>(
            &self,
            builder: ProcessBuilder<Self, Self::Input, S>,
        ) -> ProcessBuilder<Self, Self::Output, S> {
            builder.then("identity", |_token, value| value)
        }
    }

    #[test]
    fn in_memory_query_returns_values_for_step_in_insert_order() {
        let backend = InMemory::default();
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(InMemoryQueryProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(InMemoryQueryProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");

        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let step = registered
            .steps
            .get("identity")
            .expect("identity step must exist");

        let token = Token::new(storage.clone());
        storage.add(token.id(), step.clone(), 11_i32);
        storage.add(token.id(), step.clone(), 22_i32);

        let values = backend
            .query(registered, step, instance_id)
            .expect("query must succeed");

        assert_eq!(values, vec![serde_json::json!(11), serde_json::json!(22)]);
    }

    #[test]
    fn in_memory_query_returns_process_mismatch_for_other_process() {
        let backend = InMemory::default();
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(InMemoryQueryProcess)
            .expect("first process registration must succeed");
        runtime
            .register_process(InMemoryOtherProcess)
            .expect("second process registration must succeed");

        let process_name = ProcessName::from(InMemoryQueryProcess.metadata());
        let other_process_name = ProcessName::from(InMemoryOtherProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let other_registered = runtime
            .get_registered_process(&other_process_name)
            .expect("other registered process must exist");

        let instance_id: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, instance_id);
        let step = registered
            .steps
            .get("identity")
            .expect("identity step must exist");
        storage.add(Token::new(storage.clone()).id(), step, 7_i32);

        let other_step = other_registered
            .steps
            .get("identity")
            .expect("other identity step must exist");

        let error = backend
            .query(other_registered, other_step, instance_id)
            .expect_err("query must fail due to process mismatch");

        assert_eq!(error, StorageError::ProcessMismatch);
    }
}
