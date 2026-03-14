use http::{Response, StatusCode};

use crate::{InstanceSpawnError, InvalidProcessNameError, RuntimeApiError, SendError};

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
        }
    }
}

impl From<SendError> for ApiError {
    fn from(error: SendError) -> Self {
        match error {
            SendError::NoTarget => Self::conflict("no waiting instance for this message"),
            SendError::InvalidType => Self::bad_request("invalid message type"),
        }
    }
}

impl From<RuntimeApiError> for ApiError {
    fn from(error: RuntimeApiError) -> Self {
        match error {
            RuntimeApiError::Unregistered => Self::not_found("unknown process"),
            RuntimeApiError::InvalidPayload(message) => {
                Self::bad_request(format!("invalid request payload: {message}"))
            }
            RuntimeApiError::Instance(error) => error.into(),
            RuntimeApiError::Send(error) => error.into(),
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
