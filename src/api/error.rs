use http::{Response, StatusCode};
use serde::{Serialize, ser::SerializeStruct};

use super::json_response;
use crate::bpmn::{
    InstanceSpawnError, InvalidProcessNameError, messages::MessageError, storage::StorageError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    status: StatusCode,
    message: String,
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut output = serializer.serialize_struct("Error", 2)?;
        output.serialize_field("status", &self.status.as_u16())?;
        output.serialize_field("message", &self.message)?;
        output.end()
    }
}

impl schemars::JsonSchema for Error {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Error")
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "object",
            "properties": {
                "status": {
                    "type": "integer",
                    "description": "HTTP status code of the error response"
                },
                "message": {
                    "type": "string",
                    "description": "Detailed error message describing the issue",
                },
            },
            "required": ["status", "message"],
            "additionalProperties": false,
        })
    }
}

impl Error {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    pub fn method_not_allowed(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::METHOD_NOT_ALLOWED,
            message: message.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    pub fn into_response(self) -> Response<String> {
        json_response(self.status, &self)
    }
}

impl From<InstanceSpawnError> for Error {
    fn from(error: InstanceSpawnError) -> Self {
        match error {
            InstanceSpawnError::Unregistered => Self::not_found("unknown process"),
            InstanceSpawnError::Completed => Self::conflict("instance already completed"),
            InstanceSpawnError::InvalidContext => Self::bad_request("invalid process context"),
            InstanceSpawnError::InvalidInput(message) => {
                Self::bad_request(format!("invalid input: {message}"))
            }
        }
    }
}

impl From<MessageError> for Error {
    fn from(error: MessageError) -> Self {
        match error {
            MessageError::NoTarget => Self::conflict("no target for this message"),
            MessageError::InvalidType => Self::bad_request("invalid message type"),
            MessageError::Forbidden => {
                Self::forbidden("not allowed to send message in the waiting process")
            }
        }
    }
}

impl From<InvalidProcessNameError> for Error {
    fn from(error: InvalidProcessNameError) -> Self {
        Self::bad_request(format!("invalid process name: {error}"))
    }
}

impl From<uuid::Error> for Error {
    fn from(error: uuid::Error) -> Self {
        Self::bad_request(format!("invalid instance id: {error}"))
    }
}

impl From<StorageError> for Error {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::NotFound => Self::not_found("instance not found"),
            StorageError::ProcessMismatch => {
                Self::bad_request("instance belongs to a different process")
            }
            StorageError::BackendError(message) => {
                Self::bad_request(format!("storage error: {message}"))
            }
        }
    }
}
