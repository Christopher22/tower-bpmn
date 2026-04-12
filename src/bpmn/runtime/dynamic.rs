use crate::bpmn::{ExternalStep, ExternalStepData};

use super::super::{Process, Value};

/// A value which is stored on the heap.
#[derive(Debug)]
pub struct DynamicValue(Box<dyn std::any::Any + Send + Sync>);

impl DynamicValue {
    /// Creates a new dynamic value from a concrete value.
    pub fn for_process<P: Process>(value: P::Input) -> Self {
        DynamicValue(Box::new(value))
    }

    /// Try to convert a JSON value to a dynamic value for a wqinting process.
    pub fn for_value<V: Value>(value: serde_json::Value) -> Result<Self, String> {
        let value: V = serde_json::from_value(value).map_err(|e| e.to_string())?;
        Ok(DynamicValue(Box::new(value)))
    }

    /// Get the boxed value.
    pub fn to_box(self) -> Box<dyn std::any::Any + Send + Sync> {
        self.0
    }
}

impl From<DynamicValue> for Box<dyn std::any::Any + Send + Sync> {
    fn from(value: DynamicValue) -> Self {
        value.0
    }
}

/// A dynamic input which can be used to send messages with dynamic types.
#[derive(Debug, Clone)]
pub struct DynamicInput {
    step: ExternalStep,
    cast: fn(serde_json::Value) -> Result<DynamicValue, String>,
}

impl DynamicInput {
    /// Creates a new dynamic input for a given type.
    pub fn new<V: Value>(external_step: ExternalStep) -> Self {
        assert!(
            external_step.as_ref().matches::<V>(),
            "The external step does not match the expected type for this input"
        );
        DynamicInput {
            step: external_step,
            cast: |json| {
                let value: V = serde_json::from_value(json).map_err(|e| e.to_string())?;
                Ok(DynamicValue(Box::new(value)))
            },
        }
    }

    /// Try to cast the value to the expected type.
    pub fn cast(&self, json: serde_json::Value) -> Result<DynamicValue, String> {
        (self.cast)(json)
    }
}

impl std::ops::Deref for DynamicInput {
    type Target = ExternalStepData;

    fn deref(&self) -> &Self::Target {
        self.step.as_ref()
    }
}
