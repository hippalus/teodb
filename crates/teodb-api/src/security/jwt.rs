//! JWT token validation shared by REST and Flight authentication.
//!
//! Validates JWT bearer tokens against configurable issuer, audience,
//! and signing key. In production, the signing key is fetched from a
//! JWKS endpoint and cached.

use std::collections::HashMap;
use std::sync::Arc;

use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::debug;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::traits::authz::Principal;

/// Standard JWT claims used by TeoDB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeoClaims {
    /// Subject (user identifier).
    pub sub: String,
    /// Issuer.
    #[serde(default)]
    pub iss: Option<String>,
    /// Audience.
    #[serde(default)]
    pub aud: Option<StringOrVec>,
    /// Expiration (UNIX timestamp).
    #[serde(default)]
    pub exp: Option<u64>,
    /// Issued at (UNIX timestamp).
    #[serde(default)]
    pub iat: Option<u64>,
    /// Roles claim (TeoDB-specific).
    #[serde(default)]
    pub roles: Vec<String>,
    /// Additional claims.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// JWT `aud` can be a string or array of strings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrVec {
    Single(String),
    Multiple(Vec<String>),
}

/// Configuration for JWT validation.
#[derive(Debug, Clone)]
pub struct JwtValidatorConfig {
    /// Expected issuer (`iss` claim).
    pub issuer: Option<String>,
    /// Expected audience (`aud` claim).
    pub audience: Option<String>,
    /// Allowed signing algorithms.
    pub algorithms: Vec<Algorithm>,
    /// Whether to require the `exp` claim (default: true).
    pub require_exp: bool,
}

impl Default for JwtValidatorConfig {
    fn default() -> Self {
        Self {
            issuer: None,
            audience: None,
            algorithms: vec![Algorithm::RS256, Algorithm::ES256],
            require_exp: true,
        }
    }
}

/// Validates JWT tokens and extracts TeoDB principals.
pub struct JwtValidator {
    config: JwtValidatorConfig,
    /// Cached decoding key. In production, this is refreshed from a JWKS endpoint.
    decoding_key: Arc<RwLock<Option<DecodingKey>>>,
}

pub(crate) enum JwtValidationFailure {
    Expired,
    Invalid,
    ValidatorUnavailable,
}

impl JwtValidationFailure {
    pub(crate) const fn reason(&self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Invalid => "invalid",
            Self::ValidatorUnavailable => "validator_unavailable",
        }
    }

    pub(crate) fn into_error(self) -> TeoDBError {
        match self {
            Self::ValidatorUnavailable => TeoDBError::Unavailable("JWT decoding key not configured".into()),
            Self::Expired | Self::Invalid => TeoDBError::Unauthorized,
        }
    }
}

impl JwtValidator {
    /// Create a validator with a static HMAC secret (for development/testing).
    pub fn with_secret(secret: &[u8], config: JwtValidatorConfig) -> Self {
        Self {
            config,
            decoding_key: Arc::new(RwLock::new(Some(DecodingKey::from_secret(secret)))),
        }
    }

    /// Create a validator with an RSA public key in PEM format.
    pub fn with_rsa_pem(pem: &[u8], config: JwtValidatorConfig) -> TeoDBResult<Self> {
        let key = DecodingKey::from_rsa_pem(pem).map_err(|e| TeoDBError::Config(format!("invalid RSA PEM: {e}")))?;
        Ok(Self {
            config,
            decoding_key: Arc::new(RwLock::new(Some(key))),
        })
    }

    /// Create a validator with an EC public key in PEM format.
    pub fn with_ec_pem(pem: &[u8], config: JwtValidatorConfig) -> TeoDBResult<Self> {
        let key = DecodingKey::from_ec_pem(pem).map_err(|e| TeoDBError::Config(format!("invalid EC PEM: {e}")))?;
        Ok(Self {
            config,
            decoding_key: Arc::new(RwLock::new(Some(key))),
        })
    }

    /// Validate a JWT token and extract the principal.
    pub fn validate(&self, token: &str) -> TeoDBResult<Principal> {
        self.validate_classified(token)
            .map_err(JwtValidationFailure::into_error)
    }

    pub(crate) fn validate_classified(&self, token: &str) -> Result<Principal, JwtValidationFailure> {
        let key = self.decoding_key.read();
        let key = key
            .as_ref()
            .ok_or(JwtValidationFailure::ValidatorUnavailable)?;

        let mut validation = Validation::new(
            self.config
                .algorithms
                .first()
                .copied()
                .unwrap_or(Algorithm::RS256),
        );
        validation.algorithms = self.config.algorithms.clone();

        if let Some(ref issuer) = self.config.issuer {
            validation.set_issuer(&[issuer]);
        }
        if let Some(ref audience) = self.config.audience {
            validation.set_audience(&[audience]);
        }
        validation.set_required_spec_claims(&["sub"]);
        validation.validate_exp = self.config.require_exp;

        let token_data: TokenData<TeoClaims> = jsonwebtoken::decode(token, key, &validation).map_err(|error| {
            debug!(%error, "JWT validation failed");
            if matches!(error.kind(), jsonwebtoken::errors::ErrorKind::ExpiredSignature) {
                JwtValidationFailure::Expired
            } else {
                JwtValidationFailure::Invalid
            }
        })?;

        let claims = token_data.claims;
        let mut extra_claims = HashMap::new();
        for (k, v) in &claims.extra {
            if let serde_json::Value::String(s) = v {
                extra_claims.insert(k.clone(), s.clone());
            }
        }

        Ok(Principal {
            subject: claims.sub,
            roles: claims.roles,
            claims: extra_claims,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> JwtValidatorConfig {
        JwtValidatorConfig {
            issuer: Some("teodb-test".into()),
            audience: Some("teodb".into()),
            algorithms: vec![Algorithm::HS256],
            require_exp: false,
        }
    }

    fn encode_test_token(claims: &TeoClaims, secret: &[u8]) -> String {
        let header = jsonwebtoken::Header::new(Algorithm::HS256);
        let key = jsonwebtoken::EncodingKey::from_secret(secret);
        jsonwebtoken::encode(&header, claims, &key).unwrap()
    }

    #[test]
    fn valid_token_extracts_principal() {
        let secret = b"test-secret-key-at-least-32-bytes!";
        let validator = JwtValidator::with_secret(secret, test_config());

        let claims = TeoClaims {
            sub: "user@example.com".into(),
            iss: Some("teodb-test".into()),
            aud: Some(StringOrVec::Single("teodb".into())),
            exp: None,
            iat: None,
            roles: vec!["admin".into(), "reader".into()],
            extra: HashMap::new(),
        };

        let token = encode_test_token(&claims, secret);
        let principal = validator.validate(&token).unwrap();

        assert_eq!(principal.subject, "user@example.com");
        assert_eq!(principal.roles, vec!["admin", "reader"]);
    }

    #[test]
    fn invalid_signature_rejected() {
        let secret = b"test-secret-key-at-least-32-bytes!";
        let wrong_secret = b"wrong-secret-key-at-least-32-byte";
        let validator = JwtValidator::with_secret(secret, test_config());

        let claims = TeoClaims {
            sub: "user@example.com".into(),
            iss: Some("teodb-test".into()),
            aud: Some(StringOrVec::Single("teodb".into())),
            exp: None,
            iat: None,
            roles: vec![],
            extra: HashMap::new(),
        };

        let token = encode_test_token(&claims, wrong_secret);
        let result = validator.validate(&token);
        assert!(result.is_err());
    }

    #[test]
    fn wrong_issuer_rejected() {
        let secret = b"test-secret-key-at-least-32-bytes!";
        let validator = JwtValidator::with_secret(secret, test_config());

        let claims = TeoClaims {
            sub: "user@example.com".into(),
            iss: Some("wrong-issuer".into()),
            aud: Some(StringOrVec::Single("teodb".into())),
            exp: None,
            iat: None,
            roles: vec![],
            extra: HashMap::new(),
        };

        let token = encode_test_token(&claims, secret);
        let result = validator.validate(&token);
        assert!(result.is_err());
    }

    #[test]
    fn wrong_audience_rejected() {
        let secret = b"test-secret-key-at-least-32-bytes!";
        let validator = JwtValidator::with_secret(secret, test_config());

        let claims = TeoClaims {
            sub: "user@example.com".into(),
            iss: Some("teodb-test".into()),
            aud: Some(StringOrVec::Single("wrong-audience".into())),
            exp: None,
            iat: None,
            roles: vec![],
            extra: HashMap::new(),
        };

        let token = encode_test_token(&claims, secret);
        let result = validator.validate(&token);
        assert!(result.is_err());
    }

    #[test]
    fn expired_token_rejected_when_expiration_is_required() {
        let secret = b"test-secret-key-at-least-32-bytes!";
        let mut config = test_config();
        config.require_exp = true;
        let validator = JwtValidator::with_secret(secret, config);
        let claims = TeoClaims {
            sub: "user@example.com".into(),
            iss: Some("teodb-test".into()),
            aud: Some(StringOrVec::Single("teodb".into())),
            exp: Some(1),
            iat: None,
            roles: vec![],
            extra: HashMap::new(),
        };

        let token = encode_test_token(&claims, secret);
        assert!(matches!(validator.validate(&token), Err(TeoDBError::Unauthorized)));
    }

    #[test]
    fn extra_claims_extracted() {
        let secret = b"test-secret-key-at-least-32-bytes!";
        let validator = JwtValidator::with_secret(secret, test_config());

        let mut extra = HashMap::new();
        extra.insert("tenant_id".into(), serde_json::Value::String("t123".into()));

        let claims = TeoClaims {
            sub: "user@example.com".into(),
            iss: Some("teodb-test".into()),
            aud: Some(StringOrVec::Single("teodb".into())),
            exp: None,
            iat: None,
            roles: vec![],
            extra,
        };

        let token = encode_test_token(&claims, secret);
        let principal = validator.validate(&token).unwrap();
        assert_eq!(principal.claims.get("tenant_id").unwrap(), "t123");
    }
}
