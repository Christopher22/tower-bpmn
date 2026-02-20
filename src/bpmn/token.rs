use chrono::DateTime;
use dashmap::DashMap;
use std::any::TypeId;
use uuid::Uuid;

use std::{any::Any, sync::Arc};

/// Marker trait for values that can be stored in token history and messages.
pub trait Value:
    'static + Sized + Send + Sync + Any + Clone + serde::Serialize + for<'a> serde::Deserialize<'a>
{
}

impl<T> Value for T where
    T: Sized + Send + Sync + Any + Clone + serde::Serialize + for<'a> serde::Deserialize<'a>
{
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenId(Uuid);

impl TokenId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TokenId {
    fn default() -> Self {
        Self::new()
    }
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

/// A BPMN token.
#[derive(Debug)]
pub struct Token {
    ids: Vec<TokenId>,
    shared_history: Arc<DashMap<TypeId, Vec<Entry>>>,
}

impl Token {
    pub(crate) fn new() -> Self {
        Self {
            ids: vec![TokenId::new()],
            shared_history: Arc::new(DashMap::new()),
        }
    }

    /// Creates a new token. This is not a copy, but a child.
    #[allow(clippy::should_implement_trait)]
    pub fn clone(&self) -> Token {
        let mut ids = self.ids.clone();
        ids.push(TokenId::new());
        Self {
            ids,
            shared_history: self.shared_history.clone(),
        }
    }

    pub(crate) fn snapshot(&self) -> Token {
        Self {
            ids: self.ids.clone(),
            shared_history: self.shared_history.clone(),
        }
    }

    /// Returns the current branch-local token id.
    pub fn id(&self) -> TokenId {
        *self
            .ids
            .last()
            .expect("current it is always at latest place")
    }

    /// Adds a typed output value for the given step and returns the updated token.
    pub fn set_output<T: Value, S: Into<String>>(self, step: S, value: T) -> Self {
        let value = Entry::new(self.id(), step, value);
        match self.shared_history.entry(TypeId::of::<T>()) {
            dashmap::Entry::Occupied(mut entry) => {
                entry.get_mut().push(value);
            }
            dashmap::Entry::Vacant(entry) => {
                entry.insert(vec![value]);
            }
        }
        self
    }

    /// Returns the most recent value of type `T` visible in this token branch.
    pub fn get_last<T: Value>(&self) -> Option<T> {
        self.shared_history
            .get(&TypeId::of::<T>())
            .and_then(|entries| {
                entries
                    .iter()
                    .rev()
                    .find(|entry| self.ids.contains(&entry.token_id))
                    .map(|entry| {
                        entry
                            .value
                            .downcast_ref::<T>()
                            .expect("checked type")
                            .clone()
                    })
            })
    }

    pub(crate) fn current_task_name(&self) -> Option<String> {
        self.shared_history
            .iter()
            .flat_map(|typed_entries| {
                typed_entries
                    .value()
                    .iter()
                    .map(|entry| (entry.timestamp, entry.place.clone(), entry.token_id))
                    .collect::<Vec<_>>()
            })
            .filter(|(_, _, token_id)| self.ids.contains(token_id))
            .max_by_key(|(timestamp, _, _)| *timestamp)
            .map(|(_, place, _)| place)
    }
}

impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        self.ids == other.ids
    }
}

impl Eq for Token {}
