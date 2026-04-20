use std::sync::Arc;

use base64::Engine;
use http::{
    header::{self, HeaderValue},
    request::Parts,
};

use super::{Guard, OpenApiSecurityScheme};
use crate::bpmn::messages::{Context, Participant};

/// Guard that extracts HTTP Basic-Auth credentials and maps them to a context.
#[derive(Clone)]
pub struct AuthorizationGuard<F> {
    callback: Arc<F>,
}

impl<F> AuthorizationGuard<F>
where
    F: Fn(&str, &str) -> Option<Context> + Send + Sync + 'static,
{
    /// Creates a new authorization guard with a callback receiving username and password.
    pub fn new(callback: F) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }
}

impl<F> std::fmt::Debug for AuthorizationGuard<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationGuard").finish_non_exhaustive()
    }
}

impl<F> Guard for AuthorizationGuard<F>
where
    F: Fn(&str, &str) -> Option<Context> + Send + Sync + 'static,
{
    fn context_from_request(&self, request: &Parts) -> Context {
        parse_credentials(
            request
                .headers
                .get(header::AUTHORIZATION)
                .and_then(parse_basic_header),
        )
        .and_then(|(username, password)| (self.callback)(&username, &password))
        .unwrap_or_else(|| std::iter::empty::<Participant>().collect())
    }

    fn openapi_security_scheme(&self) -> Option<OpenApiSecurityScheme> {
        Some(OpenApiSecurityScheme::new(
            "basicAuth",
            serde_json::json!({
                "type": "http",
                "scheme": "basic"
            }),
            vec![],
        ))
    }
}

fn parse_basic_header(header: &HeaderValue) -> Option<&str> {
    let header = header.to_str().ok()?;
    let encoded = header.strip_prefix("Basic ")?;
    Some(encoded.trim())
}

fn parse_credentials(encoded: Option<&str>) -> Option<(String, String)> {
    let encoded = encoded?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    let decoded = std::str::from_utf8(&decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bpmn::messages::{Entity, Participant};

    fn request_with_auth(header: &str) -> Parts {
        let request = http::Request::builder()
            .header(header::AUTHORIZATION, header)
            .body(())
            .unwrap();
        request.into_parts().0
    }

    #[test]
    fn maps_valid_basic_credentials_to_context() {
        let guard = AuthorizationGuard::new(|username, password| {
            if username == "alice" && password == "secret" {
                Some(Context::for_entity(Entity::new("ApiUser")))
            } else {
                None
            }
        });
        let parts = request_with_auth("Basic YWxpY2U6c2VjcmV0");
        let context = guard.context_from_request(&parts);

        assert!(context.is_suitable_for(&Participant::Entity(Entity::new("ApiUser"))));
    }

    #[test]
    fn falls_back_to_empty_context_on_missing_or_invalid_header() {
        let guard = AuthorizationGuard::new(|_, _| Some(Context::for_entity(Entity::new("Any"))));
        let request = http::Request::builder().body(()).unwrap();
        let missing = request.into_parts().0;
        let malformed = request_with_auth("Bearer abc");

        let missing_context = guard.context_from_request(&missing);
        let malformed_context = guard.context_from_request(&malformed);

        assert!(!missing_context.is_suitable_for(&Participant::Entity(Entity::new("Any"))));
        assert!(!malformed_context.is_suitable_for(&Participant::Entity(Entity::new("Any"))));
    }
}
