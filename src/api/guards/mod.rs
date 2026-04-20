//! Guards for deriving context from incoming HTTP requests.

mod authorization;

pub use authorization::AuthorizationGuard;

use http::request::Parts;

use crate::bpmn::messages::{Context, Entity};

/// OpenAPI security scheme configuration provided by a guard.
#[derive(Debug, Clone)]
pub struct OpenApiSecurityScheme {
    /// Name of the security scheme entry in `components/securitySchemes`.
    pub name: String,
    /// Raw OpenAPI security scheme object.
    pub scheme: serde_json::Value,
    /// Scope list used for top-level `security` requirements.
    pub scopes: Vec<String>,
}

impl OpenApiSecurityScheme {
    /// Creates a new security scheme descriptor.
    pub fn new(name: impl Into<String>, scheme: serde_json::Value, scopes: Vec<String>) -> Self {
        Self {
            name: name.into(),
            scheme,
            scopes,
        }
    }
}

/// Middleware-like request guard that derives message context from incoming HTTP requests.
pub trait Guard: std::fmt::Debug + Send + Sync {
    /// Builds the context used for process/message interactions from request metadata.
    fn context_from_request(&self, request: &Parts) -> Context;

    /// Optional OpenAPI security scheme emitted into the generated specification.
    fn openapi_security_scheme(&self) -> Option<OpenApiSecurityScheme> {
        None
    }
}

/// Default guard that allows every request.
#[derive(Debug, Clone, Copy, Default)]
pub struct EverybodyGuard;

impl EverybodyGuard {
    /// Default entity used for the everybody context.
    pub const DEFAULT_ENTITY: Entity = Entity::new("Anonymous");
}

impl Guard for EverybodyGuard {
    fn context_from_request(&self, _request: &Parts) -> Context {
        Context::for_entity(Self::DEFAULT_ENTITY.clone())
    }
}
