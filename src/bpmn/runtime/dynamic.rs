use super::super::{Process, Value, messages::Participant};

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
    /// The JSON schema of the input type.
    pub json_schema: serde_json::Value,
    /// The participant responsible for providing the input, which can be used for access control and auditing.
    pub responsible: Participant,
    cast: fn(serde_json::Value) -> Result<DynamicValue, String>,
    type_id: std::any::TypeId,
}

impl DynamicInput {
    /// Create the dynamic input for a given process.
    pub fn for_process<P: Process>() -> Self {
        DynamicInput::new::<P::Input>(P::INITIAL_OWNER)
    }

    /// Creates a new dynamic input for a given type.
    pub fn new<V: Value>(responsible: Participant) -> Self {
        DynamicInput {
            json_schema: schemars::schema_for!(V).into(),
            responsible,
            cast: |json| {
                let value: V = serde_json::from_value(json).map_err(|e| e.to_string())?;
                Ok(DynamicValue(Box::new(value)))
            },
            type_id: std::any::TypeId::of::<V>(),
        }
    }

    /// Checks if the dynamic input matches a given type.
    pub fn matches<V: Value>(&self) -> bool {
        self.type_id == std::any::TypeId::of::<V>()
    }

    /// Try to cast the value to the expected type.
    pub fn cast(&self, json: serde_json::Value) -> Result<DynamicValue, String> {
        (self.cast)(json)
    }
}
