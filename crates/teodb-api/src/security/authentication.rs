use std::collections::HashMap;
use std::sync::Arc;

use axum::http::HeaderMap;
use sha2::{Digest, Sha256};
use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::traits::authz::Principal;

use crate::observer::{ApiObserver, ApiTransport};

use super::JwtValidator;

#[derive(Clone)]
pub struct AuthenticatedRequest {
    pub principal: Principal,
    pub bearer: Option<String>,
}

pub struct ApiAuthenticator {
    validator: Option<Arc<JwtValidator>>,
    observer: Arc<dyn ApiObserver>,
}

impl ApiAuthenticator {
    pub fn new(validator: Option<Arc<JwtValidator>>, observer: Arc<dyn ApiObserver>) -> Self {
        Self { validator, observer }
    }

    pub fn authenticate_headers(
        &self,
        transport: ApiTransport,
        headers: &HeaderMap,
    ) -> TeoDBResult<AuthenticatedRequest> {
        let authorization = headers.get(axum::http::header::AUTHORIZATION);
        let bearer = match authorization {
            Some(value) => match value
                .to_str()
                .ok()
                .and_then(|value| value.strip_prefix("Bearer "))
                .filter(|token| !token.is_empty())
            {
                Some(token) => Some(token),
                None => {
                    self.record(transport, "failed", "malformed");
                    return Err(TeoDBError::Unauthorized);
                }
            },
            None => None,
        };
        self.authenticate_bearer(transport, bearer)
    }

    pub fn authenticate_bearer(
        &self,
        transport: ApiTransport,
        bearer: Option<&str>,
    ) -> TeoDBResult<AuthenticatedRequest> {
        let result = match (bearer, self.validator.as_deref()) {
            (None, Some(_)) => Err((TeoDBError::Unauthorized, "missing")),
            (None, None) => Ok((anonymous_principal(), "anonymous")),
            (Some(token), Some(validator)) => validator
                .validate_classified(token)
                .map(|principal| (principal, "jwt"))
                .map_err(|failure| {
                    let reason = failure.reason();
                    (failure.into_error(), reason)
                }),
            (Some(token), None) => Ok((opaque_principal(token), "opaque")),
        };

        match result {
            Ok((principal, reason)) => {
                self.record(transport, "succeeded", reason);
                Ok(AuthenticatedRequest {
                    principal,
                    bearer: bearer.map(ToOwned::to_owned),
                })
            }
            Err((error, reason)) => {
                self.record(transport, "failed", reason);
                Err(error)
            }
        }
    }

    fn record(&self, transport: ApiTransport, outcome: &'static str, reason: &'static str) {
        self.observer
            .on_authentication(transport, outcome, reason);
    }
}

pub fn anonymous_principal() -> Principal {
    Principal {
        subject: "anonymous".into(),
        roles: Vec::new(),
        claims: HashMap::new(),
    }
}

fn opaque_principal(token: &str) -> Principal {
    let digest = Sha256::digest(token.as_bytes());
    Principal {
        subject: format!("bearer:{}", hex::encode(digest)),
        roles: Vec::new(),
        claims: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoopApiObserver;

    #[test]
    fn opaque_tokens_are_hashed() {
        let authenticator = ApiAuthenticator::new(None, Arc::new(NoopApiObserver));
        let request = authenticator
            .authenticate_bearer(ApiTransport::Rest, Some("secret"))
            .unwrap();
        assert!(request.principal.subject.starts_with("bearer:"));
        assert!(!request.principal.subject.contains("secret"));
    }

    #[test]
    fn configured_validator_requires_a_token() {
        let validator = Arc::new(JwtValidator::with_secret(
            b"test-secret-key-at-least-32-bytes!",
            Default::default(),
        ));
        let authenticator = ApiAuthenticator::new(Some(validator), Arc::new(NoopApiObserver));
        assert!(matches!(
            authenticator.authenticate_bearer(ApiTransport::Rest, None),
            Err(TeoDBError::Unauthorized)
        ));
    }
}
