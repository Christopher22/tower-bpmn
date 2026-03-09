use std::borrow::Cow;

use crate::Storage;

use super::{ProcessBuilder, Value};

/// A BPMN process definition.
pub trait Process: 'static + Sized {
    /// Input payload type for starting a process instance.
    type Input: Value;
    /// Final output payload type of the process.
    type Output: Value;

    /// Process meta data for registration and dispatch.
    fn metadata(&self) -> &MetaData;

    /// Define the process by building a process builder.
    fn define<S: Storage>(
        &self,
        builder: ProcessBuilder<Self, Self::Input, S>,
    ) -> ProcessBuilder<Self, Self::Output, S>;
}

/// Meta data for a BPMN process definition.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, schemars::JsonSchema)]
pub struct MetaData {
    /// Unique name of the process definition.
    pub name: Cow<'static, str>,
    /// Version number of the process definition.
    pub version: u32,
    /// Optional description of the process definition.
    pub description: Option<Cow<'static, str>>,
}

impl MetaData {
    /// Creates new process meta data with the given name and description.
    pub const fn new(name: &'static str, description: &'static str) -> Self {
        MetaData {
            name: Cow::Borrowed(name),
            version: 1,
            description: Some(Cow::Borrowed(description)),
        }
    }
}
