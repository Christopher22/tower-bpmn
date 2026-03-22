use http::{Response, StatusCode};

use crate::{InstanceSpawnError, InvalidProcessNameError, messages::MessageError};

use super::response::{ErrorBody, json_response};

#[derive(Debug)]
pub(super) struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub(super) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub(super) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
        }
    }

    pub(super) fn into_response(self) -> Response<String> {
        json_response(
            self.status,
            &ErrorBody {
                error: self.message,
            },
        )
    }
}

impl From<InstanceSpawnError> for ApiError {
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

impl From<MessageError> for ApiError {
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

impl From<InvalidProcessNameError> for ApiError {
    fn from(error: InvalidProcessNameError) -> Self {
        Self::bad_request(format!("invalid process name: {error}"))
    }
}

impl From<uuid::Error> for ApiError {
    fn from(error: uuid::Error) -> Self {
        Self::bad_request(format!("invalid instance id: {error}"))
    }
}
