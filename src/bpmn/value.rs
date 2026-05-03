use schemars::JsonSchema;
use std::any::Any;

pub(crate) mod internal {
    use super::*;

    /// Be default, "Value" is not a valid trait object. This basic marker is it and serve as a placeholder.
    pub trait DynValue: 'static + std::fmt::Debug + Any + Send + Sync {
        /// Clone the value into a boxed trait object.
        fn clone_box(&self) -> Box<dyn DynValue>;
    }

    impl<T> DynValue for T
    where
        T: 'static + std::fmt::Debug + Any + Send + Sync + Clone,
    {
        fn clone_box(&self) -> Box<dyn DynValue> {
            Box::new(self.clone())
        }
    }
}
/// Marker trait for values that can be stored in token history and messages.
pub trait Value:
    internal::DynValue + Clone + serde::Serialize + for<'a> serde::Deserialize<'a> + JsonSchema
{
}

impl<T> Value for T where
    T: internal::DynValue + Clone + serde::Serialize + for<'a> serde::Deserialize<'a> + JsonSchema
{
}

/// Extract specific types from values for simplying the interface.
pub trait Extract<T>: Value {
    /// Extracts the inner value of the specified type.
    fn extract(&self) -> T;
}

impl<T: Value> Extract<T> for T {
    fn extract(&self) -> T {
        self.clone()
    }
}
