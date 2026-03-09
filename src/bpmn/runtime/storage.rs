use std::{
    any::{Any, TypeId},
    collections::BTreeSet,
    sync::Arc,
};

use chrono::DateTime;
use dashmap::DashMap;

use crate::{InstanceId, TokenId, Value};

/// A backend suitable for storing the data of instances.
pub trait StorageBackend: 'static + Clone + Sized + Send + Sync {
    /// The type of storage used for instances.
    type Storage: Storage;

    /// Register a new instance.
    fn new_instance(&self, process_id: InstanceId) -> Self::Storage;
}

/// A storage for a process instance, which can be used to store token values and other data related to the instance. This is shared between all tokens in the same process instance, allowing them to see each other's values according to their branching history.
pub trait Storage: 'static + std::fmt::Debug + Clone + Send + Sync + Eq {
    /// Add a new entry to the storage for the given token ID, place, and value.
    fn add<V: Value>(&self, token_id: TokenId, place: &str, value: V);

    /// Return all the states currently active.
    fn current_places(&self) -> Vec<String>;

    /// Returns the most recent value of type `T`.
    fn get_last<T: Value>(&self, token_ids: &[TokenId]) -> Option<T>;

    /// Returns the name of the last step finished by any of the given token.
    fn last_step(&self, token_ids: &[TokenId]) -> Option<String>;
}

#[derive(Debug)]
struct Entry {
    timestamp: DateTime<chrono::Utc>,
    place: String,
    token_id: TokenId,
    /// This is enforced to be a "Value"
    value: Box<dyn Any + Send + Sync>,
}

impl Entry {
    pub fn new<S: Into<String>, V: Value>(token_id: TokenId, place: S, value: V) -> Self {
        Entry {
            timestamp: chrono::Utc::now(),
            place: place.into(),
            token_id,
            value: Box::new(value),
        }
    }
}

#[derive(Debug)]
struct RawHistory {
    entries: DashMap<TypeId, Vec<Entry>>,
    current_places: DashMap<TokenId, String>,
}

/// An in-memory storage backend, which is suitable for testing and simple use cases.
#[derive(Debug, Clone)]
pub struct InMemory(Arc<DashMap<InstanceId, Arc<RawHistory>>>);

impl Default for InMemory {
    fn default() -> Self {
        InMemory(Arc::new(DashMap::new()))
    }
}

impl StorageBackend for InMemory {
    type Storage = InMemoryStorage;

    fn new_instance(&self, process_id: InstanceId) -> Self::Storage {
        let history = Arc::new(RawHistory {
            entries: DashMap::new(),
            current_places: DashMap::new(),
        });
        self.0.insert(process_id, history.clone());
        InMemoryStorage(history)
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
    fn add<V: Value>(&self, token_id: TokenId, place: &str, value: V) {
        let data_entry = Entry::new(token_id, place, value);
        match self.0.entries.entry(TypeId::of::<V>()) {
            dashmap::Entry::Occupied(mut entry) => {
                entry.get_mut().push(data_entry);
            }
            dashmap::Entry::Vacant(entry) => {
                entry.insert(vec![data_entry]);
            }
        }
        self.0.current_places.insert(token_id, place.to_string());
    }

    fn current_places(&self) -> Vec<String> {
        self.0
            .current_places
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
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
    fn last_step(&self, token_ids: &[TokenId]) -> Option<String> {
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
