use chrono::DateTime;
use dashmap::DashMap;
use std::any::TypeId;
use uuid::Uuid;

use std::{any::Any, sync::Arc};

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
    pub fn clone(&self) -> Token {
        let mut ids = self.ids.clone();
        ids.push(TokenId::new());
        Self {
            ids,
            shared_history: self.shared_history.clone(),
        }
    }

    pub fn id(&self) -> TokenId {
        *self
            .ids
            .last()
            .expect("current it is always at latest place")
    }

    pub fn set_output<T: Value, S: Into<String>>(self, step: S, value: T) -> Self {
        let value = Entry::new(self.id(), step, value);
        match self.shared_history.entry(std::any::TypeId::of::<T>()) {
            dashmap::Entry::Occupied(mut entry) => {
                entry.get_mut().push(value);
            }
            dashmap::Entry::Vacant(entry) => {
                entry.insert(vec![value]);
            }
        }
        self
    }

    pub fn get_last<T: Value>(&self) -> Option<T> {
        self.shared_history
            .get(&std::any::TypeId::of::<T>())
            .and_then(|entry| {
                for entry in entry.iter() {
                    if self.ids.contains(&entry.token_id) {
                        return Some(
                            entry
                                .value
                                .downcast_ref::<T>()
                                .expect("checked type")
                                .clone(),
                        );
                    }
                }
                None
            })
    }
}

impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        self.ids == other.ids
    }
}

impl Eq for Token {}
