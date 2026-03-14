use schemars::JsonSchema;
use uuid::Uuid;

use std::any::Any;

use crate::{Step, Storage};

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

/// ID of a BPMN token, which is used for tracking token history and visibility across branches in the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenId(Uuid);

impl TokenId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// A BPMN token.
pub struct Token<S> {
    ids: Vec<TokenId>,
    storage: S,
}

impl<S> std::fmt::Debug for Token<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Token")
            .field("ids", &self.ids)
            .finish_non_exhaustive()
    }
}

impl<S: Storage> Token<S> {
    pub(crate) fn new(storage: S) -> Self {
        Self {
            ids: vec![TokenId::new()],
            storage,
        }
    }

    /// Creates a new token. This is not a copy, but a child.
    pub fn fork(&self) -> Token<S> {
        let mut ids = self.ids.clone();
        ids.push(TokenId::new());
        Self {
            ids,
            storage: self.storage.clone(),
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
    pub fn set_output<T: Value>(self, step: Step, value: T) -> Self {
        self.storage.add(self.id(), step, value);
        self
    }

    /// Returns the most recent value of type `T` visible in this token branch.
    pub fn get_last<T: Value>(&self) -> Option<T> {
        self.storage.get_last(&self.ids)
    }

    /// Returns the name of the last step finished by this token.
    pub fn last_step(&self) -> Option<Step> {
        self.storage.last_step(&self.ids)
    }
}

impl<S> PartialEq for Token<S> {
    fn eq(&self, other: &Self) -> bool {
        self.ids == other.ids
    }
}

impl<S> Eq for Token<S> {}

impl<S: Clone> Clone for Token<S> {
    fn clone(&self) -> Self {
        Token {
            ids: self.ids.clone(),
            storage: self.storage.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{InMemoryStorage, Steps};

    use super::*;

    #[test]
    fn token_history_returns_latest_value_and_task() {
        let steps = Steps::new(["step-a", "step-b", "step-c"].into_iter()).unwrap();
        let token = Token::new(InMemoryStorage::for_test())
            .set_output(steps.get("step-a").unwrap(), 1_i32)
            .set_output(steps.get("step-b").unwrap(), 2_i32)
            .set_output(steps.get("step-c").unwrap(), 3_i32);

        assert_eq!(token.get_last::<i32>(), Some(3));
        assert_eq!(token.last_step(), Some(steps.get("step-c").unwrap()));
    }

    #[test]
    fn token_child_branch_keeps_parent_history_visible() {
        let steps = Steps::new(["root", "child"].into_iter()).unwrap();
        let root =
            Token::new(InMemoryStorage::for_test()).set_output(steps.get("root").unwrap(), 5_i32);
        let child = root.fork().set_output(steps.get("child").unwrap(), 9_i32);

        assert_eq!(root.get_last::<i32>(), Some(5));
        assert_eq!(child.get_last::<i32>(), Some(9));
    }

    #[test]
    fn shared_history_tracks_current_places_for_active_branches() {
        let steps = Steps::new(["start", "root-step", "child-step"].into_iter()).unwrap();
        let root = Token::new(InMemoryStorage::for_test())
            .set_output(steps.get("start").unwrap(), 1_i32)
            .set_output(steps.get("root-step").unwrap(), 2_i32);
        let child = root
            .fork()
            .set_output(steps.get("child-step").unwrap(), 3_i32);

        let current_places = root.storage.current_places();
        assert_eq!(current_places.len(), 2);
        assert!(current_places.contains(&steps.get("root-step").unwrap()));
        assert!(current_places.contains(&steps.get("child-step").unwrap()));

        assert_eq!(child.last_step(), Some(steps.get("child-step").unwrap()));
    }
}
