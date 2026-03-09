use chrono::DateTime;
use dashmap::DashMap;
use schemars::JsonSchema;
use std::any::TypeId;
use std::collections::BTreeSet;
use uuid::Uuid;

use std::{any::Any, sync::Arc};

/// Marker trait for values that can be stored in token history and messages.
pub trait Value:
    'static
    + Sized
    + Send
    + Sync
    + Any
    + Clone
    + serde::Serialize
    + for<'a> serde::Deserialize<'a>
    + JsonSchema
{
}

impl<T> Value for T where
    T: Sized
        + Send
        + Sync
        + Any
        + Clone
        + serde::Serialize
        + for<'a> serde::Deserialize<'a>
        + JsonSchema
{
}

pub type TokenObserver = Arc<dyn Fn(TokenId, &str) + Send + Sync>;

/// ID of a BPMN token, which is used for tracking token history and visibility across branches in the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenId(Uuid);

impl TokenId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
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

/// A (shared) history of token values, indexed by type and token id, with timestamps for determining the current value at a given place in the process. This is shared between all tokens in the same process instance, allowing them to see each other's values according to their branching history.
#[derive(Clone)]
pub struct SharedHistory(
    Arc<DashMap<TypeId, Vec<Entry>>>,
    Option<TokenObserver>,
    Arc<DashMap<TokenId, String>>,
);

impl std::fmt::Debug for SharedHistory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedHistory").finish_non_exhaustive()
    }
}

impl SharedHistory {
    /// Creates a new shared history with no entries.
    pub fn new() -> Self {
        SharedHistory(Arc::new(DashMap::new()), None, Arc::new(DashMap::new()))
    }

    /// Register a observer tracking changes.
    pub fn with_observer(mut self, observer: TokenObserver) -> Self {
        self.1 = Some(observer);
        self
    }

    fn add<V: Value>(&self, token_id: TokenId, place: &str, value: V) {
        let data_entry = Entry::new(token_id, place, value);
        match self.0.entry(TypeId::of::<V>()) {
            dashmap::Entry::Occupied(mut entry) => {
                entry.get_mut().push(data_entry);
            }
            dashmap::Entry::Vacant(entry) => {
                entry.insert(vec![data_entry]);
            }
        }

        self.2.insert(token_id, place.to_string());

        if let Some(observer) = &self.1 {
            observer(token_id, place);
        }
    }

    /// Returns the set of places where this instance currently has at least one token branch.
    pub fn current_places(&self) -> Vec<String> {
        self.2
            .iter()
            .map(|entry| entry.value().clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

/// A BPMN token.
pub struct Token {
    ids: Vec<TokenId>,
    shared_history: SharedHistory,
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Token")
            .field("ids", &self.ids)
            .finish_non_exhaustive()
    }
}

impl Token {
    pub(crate) fn new(shared_history: SharedHistory) -> Self {
        Self {
            ids: vec![TokenId::new()],
            shared_history,
        }
    }

    /// Creates a new token. This is not a copy, but a child.
    pub fn fork(&self) -> Token {
        let mut ids = self.ids.clone();
        ids.push(TokenId::new());
        Self {
            ids,
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
    pub fn set_output<T: Value, S: AsRef<str>>(self, step: S, value: T) -> Self {
        let step = step.as_ref();
        self.shared_history.add(self.id(), step, value);
        self
    }

    /// Returns the most recent value of type `T` visible in this token branch.
    pub fn get_last<T: Value>(&self) -> Option<T> {
        self.shared_history
            .0
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

    /// Returns the name of the last step finished by this token.
    pub fn last_step(&self) -> Option<String> {
        self.shared_history
            .0
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_history_returns_latest_value_and_task() {
        let token = Token::new(SharedHistory::new())
            .set_output("step-a", 1_i32)
            .set_output("step-b", 2_i32)
            .set_output("step-c", 3_i32);

        assert_eq!(token.get_last::<i32>(), Some(3));
        assert_eq!(token.last_step(), Some("step-c".into()));
    }

    #[test]
    fn token_child_branch_keeps_parent_history_visible() {
        let root = Token::new(SharedHistory::new()).set_output("root", 5_i32);
        let child = root.fork().set_output("child", 9_i32);

        assert_eq!(root.get_last::<i32>(), Some(5));
        assert_eq!(child.get_last::<i32>(), Some(9));
    }

    #[test]
    fn shared_history_tracks_current_places_for_active_branches() {
        let root = Token::new(SharedHistory::new())
            .set_output("start", 1_i32)
            .set_output("root-step", 2_i32);
        let child = root.fork().set_output("child-step", 3_i32);

        assert_eq!(
            root.shared_history.current_places(),
            vec!["child-step".to_string(), "root-step".to_string()]
        );
        assert_eq!(child.last_step(), Some("child-step".into()));
    }
}
