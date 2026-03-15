#[derive(
    Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
/// A participant in a BPMN process, which can be used for assigning tasks and sending messages to specific entities or roles.
pub enum Participant {
    /// No specific participant which could be used to deactivate processes.
    Nobody,
    /// A specific entity, such as a department.
    Entity(String),
    /// A role, like a chef.
    Role(String),
    /// A generic participant representing everyone.
    Everyone,
}
