use std::borrow::Cow;

use crate::Storage;

use super::{ProcessBuilder, Value, messages::Participant};

/// A BPMN process definition.
pub trait Process: 'static + Clone + Sized + Send + Sync {
    /// Input payload type for starting a process instance.
    type Input: Value;

    /// Final output payload type of the process.
    type Output: Value;

    /// Initial owner of process instances, which determines which participants can start the process.
    const INITIAL_OWNER: Participant = Participant::Everyone;

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

/// A unique name for a process definition, which remains valid across different versions of the same process and (re-)starts of the engine.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProcessName(Cow<'static, str>, u32);

impl ProcessName {
    const SEPARATOR: char = '-';
}

impl<'a> From<&'a MetaData> for ProcessName {
    fn from(value: &'a MetaData) -> Self {
        ProcessName(value.name.clone(), value.version)
    }
}

impl std::fmt::Display for ProcessName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{}", self.0, ProcessName::SEPARATOR, self.1)
    }
}

impl std::str::FromStr for ProcessName {
    type Err = InvalidProcessNameError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.rsplitn(2, ProcessName::SEPARATOR);
        let (version, name) = (parts.next(), parts.next());
        match (name, version) {
            (Some(name), Some(version)) if !name.is_empty() => {
                let version = version
                    .parse::<u32>()
                    .map_err(|_| InvalidProcessNameError::InvalidVersion)?;
                Ok(ProcessName(Cow::Owned(name.to_string()), version))
            }
            (Some(_), None) => Err(InvalidProcessNameError::MissingSeparator),
            (Some(""), Some(_)) => Err(InvalidProcessNameError::EmptyName),
            _ => Err(InvalidProcessNameError::MissingSeparator),
        }
    }
}

impl serde::Serialize for ProcessName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for ProcessName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Visitor;

        struct ProcessNameVisitor;

        impl<'de> Visitor<'de> for ProcessNameVisitor {
            type Value = ProcessName;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string representing an ProcessName")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                std::str::FromStr::from_str(s).map_err(serde::de::Error::custom)
            }
        }

        deserializer.deserialize_str(ProcessNameVisitor)
    }
}

impl schemars::JsonSchema for ProcessName {
    fn schema_name() -> Cow<'static, str> {
        "ProcessName".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        generator.subschema_for::<String>()
    }
}

/// Errors that can occur when trying to parse a process name from a string.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InvalidProcessNameError {
    /// The string does not contain the required separator between name and version.
    MissingSeparator,
    /// The version part of the string is not a valid unsigned integer.
    EmptyName,
    /// The name part of the string is empty.
    InvalidVersion,
}

impl std::fmt::Display for InvalidProcessNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InvalidProcessNameError::MissingSeparator => write!(
                f,
                "Invalid process name: missing separator '{}'",
                ProcessName::SEPARATOR
            ),
            InvalidProcessNameError::EmptyName => write!(f, "Invalid process name: empty name"),
            InvalidProcessNameError::InvalidVersion => {
                write!(f, "Invalid process name: invalid version")
            }
        }
    }
}

impl std::error::Error for InvalidProcessNameError {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_metadata_new() {
        let meta = MetaData::new("order-process", "Handles incoming orders");
        assert_eq!(meta.name, "order-process");
        assert_eq!(meta.version, 1);
        assert_eq!(
            meta.description,
            Some(Cow::Borrowed("Handles incoming orders"))
        );
    }

    #[test]
    fn test_process_name_from_metadata() {
        let meta = MetaData::new("payment-auth", "Authorization logic");
        let process_name = ProcessName::from(&meta);
        assert_eq!(process_name.to_string(), "payment-auth-1");
    }

    #[test]
    fn test_process_name_display() {
        let name = ProcessName(Cow::Borrowed("test-service"), 5);
        assert_eq!(format!("{}", name), "test-service-5");
    }

    #[test]
    fn test_process_name_from_str_valid() {
        let input = "my-cool-process-42";
        let parsed: ProcessName = input.parse().unwrap();

        assert_eq!(parsed.0, "my-cool-process");
        assert_eq!(parsed.1, 42);
    }

    #[test]
    fn test_process_name_from_str_invalid_formats() {
        let no_sep = "invalidname";
        assert_eq!(
            no_sep.parse::<ProcessName>().unwrap_err(),
            InvalidProcessNameError::MissingSeparator
        );

        let bad_version = "process-v1";
        assert_eq!(
            bad_version.parse::<ProcessName>().unwrap_err(),
            InvalidProcessNameError::InvalidVersion
        );
    }

    #[test]
    fn test_serde_serialization() {
        let name = ProcessName(Cow::Borrowed("order-flow"), 3);
        let serialized = serde_json::to_string(&name).unwrap();

        assert_eq!(serialized, "\"order-flow-3\"");
    }

    #[test]
    fn test_serde_deserialization() {
        let json = "\"inventory-check-10\"";
        let deserialized: ProcessName = serde_json::from_str(json).unwrap();

        assert_eq!(deserialized.0, "inventory-check");
        assert_eq!(deserialized.1, 10);
    }
}
