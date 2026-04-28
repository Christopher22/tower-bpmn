use std::sync::Arc;

use http::{header, request::Parts};
use jsonwebtoken::{DecodingKey, Validation};
use serde::Deserialize;

use super::{Guard, OpenApiSecurityScheme};
use crate::bpmn::messages::{Context, Entity, Participant, Role};

/// Standard OIDC claims extracted from a Bearer JWT.
#[derive(Debug, Deserialize)]
struct OidcClaims {
    sub: String,
    preferred_username: Option<String>,
    groups: Option<Vec<String>>,
}

/// Guard that validates OIDC Bearer tokens and maps standard JWT claims to a context.
///
/// The `preferred_username` claim (falling back to `sub`) is mapped to the entity,
/// and each entry in the `groups` claim is mapped to a role.
pub struct OidcGuard {
    decoding_key: Arc<DecodingKey>,
    validation: Arc<Validation>,
}

impl OidcGuard {
    /// Creates a new OIDC guard with the given decoding key and validation parameters.
    pub fn new(decoding_key: DecodingKey, validation: Validation) -> Self {
        Self {
            decoding_key: Arc::new(decoding_key),
            validation: Arc::new(validation),
        }
    }
}

impl std::fmt::Debug for OidcGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcGuard").finish_non_exhaustive()
    }
}

impl Guard for OidcGuard {
    fn context_from_request(&self, request: &Parts) -> Context {
        let token = request
            .headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));

        let Some(token) = token else {
            return std::iter::empty::<Participant>().collect();
        };

        let claims =
            match jsonwebtoken::decode::<OidcClaims>(token, &self.decoding_key, &self.validation) {
                Ok(data) => data.claims,
                Err(_) => return std::iter::empty::<Participant>().collect(),
            };

        let username = claims.preferred_username.unwrap_or(claims.sub);
        let mut participants: Vec<Participant> = vec![Participant::Entity(Entity::from(username))];

        if let Some(groups) = claims.groups {
            participants.extend(groups.into_iter().map(|g| Participant::Role(Role::from(g))));
        }

        participants.into_iter().collect()
    }

    fn openapi_security_scheme(&self) -> Option<OpenApiSecurityScheme> {
        Some(OpenApiSecurityScheme::new(
            "oidcBearer",
            serde_json::json!({
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT"
            }),
            vec![],
        ))
    }
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, Validation};
    use serde::Serialize;

    use super::*;
    use crate::bpmn::messages::{Entity, Participant, Role};

    #[derive(Serialize)]
    struct TestClaims {
        sub: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        preferred_username: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        groups: Option<Vec<String>>,
        exp: u64,
    }

    fn encode_token(claims: &TestClaims, secret: &[u8]) -> String {
        jsonwebtoken::encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    fn make_validation(secret: &[u8]) -> (DecodingKey, Validation) {
        let key = DecodingKey::from_secret(secret);
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = false;
        (key, validation)
    }

    fn request_with_bearer(token: &str) -> Parts {
        http::Request::builder()
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(())
            .unwrap()
            .into_parts()
            .0
    }

    #[test]
    fn maps_preferred_username_to_entity() {
        let secret = b"test-secret";
        let claims = TestClaims {
            sub: "user-id-42".into(),
            preferred_username: Some("alice".into()),
            groups: None,
            exp: 9999999999,
        };
        let token = encode_token(&claims, secret);
        let (key, validation) = make_validation(secret);
        let guard = OidcGuard::new(key, validation);

        let context = guard.context_from_request(&request_with_bearer(&token));

        assert!(context.is_suitable_for(&Participant::Entity(Entity::from("alice".to_string()))));
        assert!(
            !context.is_suitable_for(&Participant::Entity(Entity::from("user-id-42".to_string())))
        );
    }

    #[test]
    fn falls_back_to_sub_when_preferred_username_absent() {
        let secret = b"test-secret";
        let claims = TestClaims {
            sub: "user-id-42".into(),
            preferred_username: None,
            groups: None,
            exp: 9999999999,
        };
        let token = encode_token(&claims, secret);
        let (key, validation) = make_validation(secret);
        let guard = OidcGuard::new(key, validation);

        let context = guard.context_from_request(&request_with_bearer(&token));

        assert!(
            context.is_suitable_for(&Participant::Entity(Entity::from("user-id-42".to_string())))
        );
    }

    #[test]
    fn maps_groups_to_roles() {
        let secret = b"test-secret";
        let claims = TestClaims {
            sub: "user-id-1".into(),
            preferred_username: Some("bob".into()),
            groups: Some(vec!["admins".into(), "editors".into()]),
            exp: 9999999999,
        };
        let token = encode_token(&claims, secret);
        let (key, validation) = make_validation(secret);
        let guard = OidcGuard::new(key, validation);

        let context = guard.context_from_request(&request_with_bearer(&token));

        assert!(context.is_suitable_for(&Participant::Role(Role::from("admins".to_string()))));
        assert!(context.is_suitable_for(&Participant::Role(Role::from("editors".to_string()))));
        assert!(!context.is_suitable_for(&Participant::Role(Role::from("superusers".to_string()))));
    }

    #[test]
    fn returns_empty_context_for_missing_or_invalid_token() {
        let secret = b"test-secret";
        let (key, validation) = make_validation(secret);
        let guard = OidcGuard::new(key, validation);

        let no_auth = http::Request::builder().body(()).unwrap().into_parts().0;
        let wrong_scheme = http::Request::builder()
            .header(header::AUTHORIZATION, "Basic dXNlcjpwYXNz")
            .body(())
            .unwrap()
            .into_parts()
            .0;
        let bad_token = request_with_bearer("not.a.valid.jwt");

        for parts in [no_auth, wrong_scheme, bad_token] {
            let ctx = guard.context_from_request(&parts);
            assert!(!ctx.is_suitable_for(&Participant::Entity(Entity::from("anyone".to_string()))));
        }
    }
}
