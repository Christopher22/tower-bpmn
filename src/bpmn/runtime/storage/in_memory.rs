use std::{
    any::{Any, TypeId},
    sync::Arc,
};

use chrono::DateTime;
use dashmap::DashMap;
use serde_json::Value as JsonValue;

use crate::bpmn::{
    InstanceId, ProcessName, RegisteredProcess, Step, Storage, TokenId, Value,
    messages::Entity,
    storage::{FinishedStep, InstanceDetails, ResumableProcess, StorageBackend, StorageError},
    value::internal::DynValue,
};

/// A stored value after a process finished.
#[derive(Debug)]
pub struct Entry {
    pub timestamp: DateTime<chrono::Utc>,
    pub responsible: Entity,
    pub place: Step,
    token_id: TokenId,
    /// This is enforced to be a "Value"
    value: Box<dyn DynValue>,
    serialize_json: fn(&dyn Any) -> JsonValue,
}

impl Entry {
    pub(crate) fn new<V: Value>(token_id: TokenId, place: Step, owner: Entity, value: V) -> Self {
        Entry {
            timestamp: chrono::Utc::now(),
            place,
            token_id,
            responsible: owner,
            value: Box::new(value),
            serialize_json: Self::serialize_json_impl::<V>,
        }
    }

    fn serialize_json_impl<V: Value>(value: &dyn Any) -> JsonValue {
        let typed = value.downcast_ref::<V>().expect("checked type");
        serde_json::to_value(typed).expect("value must serialize")
    }
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Entry {
            timestamp: self.timestamp,
            place: self.place.clone(),
            token_id: self.token_id,
            responsible: self.responsible.clone(),
            value: self.value.clone_box(),
            serialize_json: self.serialize_json,
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

impl InMemory {
    /// Query the full history of a process instance, returning all entries ordered by insertion time.
    pub fn query_history(&self, instance_id: InstanceId) -> Result<Vec<Entry>, StorageError> {
        let mut history: Vec<_> = match self.0.get(&instance_id) {
            Some(instance) => Ok(instance
                .history
                .entries
                .iter()
                .flat_map(|typed_entries| typed_entries.to_vec())
                .collect()),
            None => Err(StorageError::NotFound),
        }?;
        history.sort_by_key(|entry| entry.timestamp);
        Ok(history)
    }
}

impl StorageBackend for InMemory {
    type Storage = InMemoryStorage;

    fn query(
        &self,
        process: &RegisteredProcess<Self>,
        step: Step,
        instance_id: InstanceId,
    ) -> Result<Vec<FinishedStep>, StorageError> {
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
                            entry.responsible.clone(),
                            (entry.serialize_json)(entry.value.as_ref()),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        rows.sort_by_key(|(timestamp, _, _)| *timestamp);

        Ok(rows
            .into_iter()
            .map(|(timestamp, responsible, output)| FinishedStep {
                timestamp,
                responsible,
                output,
            })
            .collect())
    }

    fn query_all(
        &self,
        process: &RegisteredProcess<Self>,
    ) -> Result<Vec<InstanceDetails>, StorageError> {
        let expected_process = ProcessName::from(&process.meta_data);

        let mut details = self
            .0
            .iter()
            .filter_map(|instance| {
                if instance.value().process_name != expected_process {
                    return None;
                }

                let latest = instance
                    .value()
                    .history
                    .entries
                    .iter()
                    .flat_map(|typed_entries| {
                        typed_entries
                            .value()
                            .iter()
                            .map(|entry| {
                                (
                                    entry.timestamp,
                                    entry.place.clone(),
                                    entry.responsible.clone(),
                                    (entry.serialize_json)(entry.value.as_ref()),
                                )
                            })
                            .collect::<Vec<_>>()
                    })
                    .max_by_key(|(timestamp, _, _, _)| *timestamp)?;

                Some(InstanceDetails {
                    instance_id: instance.key().clone(),
                    step: latest.1,
                    data: FinishedStep {
                        timestamp: latest.0,
                        responsible: latest.2,
                        output: latest.3,
                    },
                })
            })
            .collect::<Vec<_>>();

        // Keep output deterministic for tests and API consumers.
        details.sort_by(|left, right| {
            left.data
                .timestamp
                .cmp(&right.data.timestamp)
                .then_with(|| {
                    left.instance_id
                        .to_string()
                        .cmp(&right.instance_id.to_string())
                })
        });

        Ok(details)
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

/// A (shared) history of token values.
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
    fn add<V: Value>(&self, responsible: &Entity, token_id: TokenId, place: Step, value: V) {
        let data_entry = Entry::new(token_id, place.clone(), responsible.clone(), value);
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
                    let any: &dyn Any = entry.value.as_ref();
                    any.downcast_ref::<T>().expect("checked type").clone()
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

        let token = Token::new(Entity::SYSTEM, storage.clone());
        storage.add(&token.responsible, token.id(), step.clone(), 11_i32);
        storage.add(&token.responsible, token.id(), step.clone(), 22_i32);

        let finished_steps = backend
            .query(registered, step, instance_id)
            .expect("query must succeed");

        assert_eq!(finished_steps.len(), 2);
        assert_eq!(finished_steps[0].output, serde_json::json!(11));
        assert_eq!(finished_steps[1].output, serde_json::json!(22));
        assert_eq!(finished_steps[0].responsible, Entity::SYSTEM);
        assert_eq!(finished_steps[1].responsible, Entity::SYSTEM);
        // Verify timestamps are in chronological order
        assert!(finished_steps[0].timestamp <= finished_steps[1].timestamp);
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
        storage.add(
            &Entity::SYSTEM,
            Token::new(Entity::SYSTEM, storage.clone()).id(),
            step,
            7_i32,
        );

        let other_step = other_registered
            .steps
            .get("identity")
            .expect("other identity step must exist");

        let error = backend
            .query(other_registered, other_step, instance_id)
            .expect_err("query must fail due to process mismatch");

        assert_eq!(error, StorageError::ProcessMismatch);
    }

    #[test]
    fn in_memory_query_returns_finished_steps_with_responsible_and_timestamp() {
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

        let alice = Entity::new("Alice");
        let charlie = Entity::new("Charlie");

        let alice_token = Token::new(alice.clone(), storage.clone());
        let charlie_token = Token::new(charlie.clone(), storage.clone());

        storage.add(&alice, alice_token.id(), step.clone(), 100_i32);
        storage.add(&charlie, charlie_token.id(), step.clone(), 200_i32);

        let finished_steps = backend
            .query(registered, step, instance_id)
            .expect("query must succeed");

        assert_eq!(finished_steps.len(), 2);

        // First step added by Alice
        assert_eq!(finished_steps[0].output, serde_json::json!(100));
        assert_eq!(finished_steps[0].responsible, alice);

        // Second step added by Charlie
        assert_eq!(finished_steps[1].output, serde_json::json!(200));
        assert_eq!(finished_steps[1].responsible, charlie);

        // Verify timestamps are present and ordered
        assert!(finished_steps[0].timestamp <= finished_steps[1].timestamp);
    }

    #[test]
    fn in_memory_query_all_returns_latest_state_per_instance() {
        let backend = InMemory::default();
        let mut runtime = Runtime::new(TokioExecutor, backend.clone());
        runtime
            .register_process(InMemoryQueryProcess)
            .expect("process registration must succeed");

        let process_name = ProcessName::from(InMemoryQueryProcess.metadata());
        let registered = runtime
            .get_registered_process(&process_name)
            .expect("registered process must exist");
        let step = registered
            .steps
            .get("identity")
            .expect("identity step must exist");

        let instance_a: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");
        let instance_b: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage_a = backend.new_instance(registered, instance_a);
        let storage_b = backend.new_instance(registered, instance_b);

        let token_a = Token::new(Entity::SYSTEM, storage_a.clone());
        let token_b = Token::new(Entity::SYSTEM, storage_b.clone());

        storage_a.add(&Entity::SYSTEM, token_a.id(), step.clone(), 10_i32);
        storage_a.add(&Entity::SYSTEM, token_a.id(), step.clone(), 20_i32);
        storage_b.add(&Entity::SYSTEM, token_b.id(), step.clone(), 30_i32);

        let all = backend
            .query_all(registered)
            .expect("query_all must succeed");

        assert_eq!(all.len(), 2);

        let row_a = all
            .iter()
            .find(|row| row.instance_id == instance_a)
            .expect("instance a must be present");
        let row_b = all
            .iter()
            .find(|row| row.instance_id == instance_b)
            .expect("instance b must be present");

        assert_eq!(row_a.step.as_str(), step.as_str());
        assert_eq!(row_a.data.output, serde_json::json!(20));
        assert_eq!(row_b.step.as_str(), step.as_str());
        assert_eq!(row_b.data.output, serde_json::json!(30));
    }

    #[test]
    fn in_memory_query_all_filters_other_processes() {
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

        let first_step = registered
            .steps
            .get("identity")
            .expect("identity step must exist");
        let other_step = other_registered
            .steps
            .get("identity")
            .expect("other identity step must exist");

        let first_instance: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");
        let other_instance: InstanceId = uuid::Uuid::new_v4()
            .to_string()
            .parse()
            .expect("uuid must parse to instance id");

        let storage = backend.new_instance(registered, first_instance);
        storage.add(
            &Entity::SYSTEM,
            Token::new(Entity::SYSTEM, storage.clone()).id(),
            first_step,
            7_i32,
        );

        let other_storage = backend.new_instance(other_registered, other_instance);
        other_storage.add(
            &Entity::SYSTEM,
            Token::new(Entity::SYSTEM, other_storage.clone()).id(),
            other_step,
            8_i32,
        );

        let all = backend
            .query_all(registered)
            .expect("query_all must succeed");

        assert_eq!(all.len(), 1);
        assert_eq!(all[0].instance_id, first_instance);
        assert_eq!(all[0].data.output, serde_json::json!(7));
    }
}
