//! Per-request security context and router-level authorization guards.
//!
//! Token extraction, parsing, and JWT validation happen exactly once per
//! request, inside the [`SecurityContext`] extractor (the axum
//! `FromRequestParts` pattern). Handlers declare `ctx: SecurityContext` and
//! call [`SecurityContext::authorize`] with their action/resource; uniform
//! surfaces (admin endpoints, `/metrics`) are guarded at the router level
//! with [`admin_guard`] instead of per-handler calls.
//!
//! Denials are centralized: every rejection renders as an RFC 9457
//! ProblemDetail through [`Denied`]'s `IntoResponse`.

use std::sync::Arc;

use axum::extract::{FromRequestParts, OriginalUri, State};
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use tracing::info;

use teodb_core::error::TeoDBError;
use teodb_core::traits::authz::{Action, Principal, Resource};

use super::problem::problem_from_error;
use crate::http::state::AppState;
use crate::observer::ApiTransport;
use crate::security::AuthenticatedRequest;

/// Authenticated context of one request: the (JWT-validated) principal, the
/// raw bearer token, and the request path for problem-detail responses.
#[derive(Clone)]
pub struct SecurityContext {
    principal: Principal,
    bearer: Option<String>,
    instance: String,
    state: Arc<AppState>,
}

/// An authorization denial. Renders as an RFC 9457 ProblemDetail with the
/// request path as `instance` — the single place auth failures become HTTP.
pub struct Denied {
    error: TeoDBError,
    instance: String,
}

impl IntoResponse for Denied {
    fn into_response(self) -> Response {
        problem_from_error(self.error, &self.instance).into_response()
    }
}

impl Denied {
    /// The RFC 9457 problem detail for this denial, with the request path
    /// already set as `instance`. Used by `ApiError`'s `From<Denied>` so a
    /// handler can surface an authorization failure with bare `?`.
    pub(crate) fn into_problem(self) -> teodb_core::problem::ProblemDetail {
        self.error
            .to_problem_detail()
            .with_instance(&self.instance)
    }
}

impl SecurityContext {
    /// The request's principal (JWT-validated when a validator is configured,
    /// opaque-bearer or anonymous otherwise).
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Authorize `action` on `resource` against the configured authorizer.
    /// Allowed when no authorizer is configured (anonymous/plaintext mode).
    pub async fn authorize(&self, action: Action, resource: Resource) -> Result<(), Denied> {
        if let Err(error) = self
            .state
            .security
            .authorization
            .authorize(ApiTransport::Rest, &self.principal, action.clone(), &resource)
            .await
        {
            self.audit(&action, &resource, "denied");
            return Err(Denied {
                error,
                instance: self.instance.clone(),
            });
        }
        self.audit(&action, &resource, "allowed");
        Ok(())
    }

    /// Require the configured admin bearer token (`security.admin_token`).
    ///
    /// Allowed when no token is configured — the open default preserves
    /// pre-existing dev/standalone behavior (the server warns at startup).
    pub fn require_admin_token(&self) -> Result<(), Denied> {
        let Some(expected) = self.state.security.admin_token.as_deref() else {
            return Ok(());
        };

        match self.bearer.as_deref() {
            Some(token) if constant_time_eq(token.as_bytes(), expected.as_bytes()) => Ok(()),
            _ => {
                info!(
                    target: "teodb::audit",
                    instance = %self.instance,
                    outcome = "denied",
                    "admin token check failed"
                );
                Err(Denied {
                    error: TeoDBError::Unauthorized,
                    instance: self.instance.clone(),
                })
            }
        }
    }

    fn extract(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Denied> {
        let instance = parts
            .extensions
            .get::<OriginalUri>()
            .map_or_else(|| parts.uri.path().to_string(), |o| o.0.path().to_string());

        let authenticated = match parts
            .extensions
            .get::<AuthenticatedRequest>()
            .cloned()
        {
            Some(authenticated) => authenticated,
            None => state
                .security
                .authenticator
                .authenticate_headers(ApiTransport::Rest, &parts.headers)
                .map_err(|error| Denied {
                    error,
                    instance: instance.clone(),
                })?,
        };

        Ok(Self {
            principal: authenticated.principal,
            bearer: authenticated.bearer,
            instance,
            state: state.clone(),
        })
    }

    fn audit(&self, action: &Action, resource: &Resource, outcome: &str) {
        info!(
            target: "teodb::audit",
            subject = %self.principal.subject,
            action = ?action,
            resource = ?resource,
            outcome,
            "audit"
        );
    }
}

impl FromRequestParts<Arc<AppState>> for SecurityContext {
    type Rejection = Denied;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        Self::extract(parts, state)
    }
}

/// Router-level guard for admin surfaces (`/api/v1/admin/*`, `/metrics`):
/// requires the admin bearer token (when configured) and `Action::Admin` on
/// the cluster (when an authorizer is configured).
///
/// Apply with `route_layer(middleware::from_fn_with_state(state, admin_guard))`.
pub async fn admin_guard(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let (mut parts, body) = req.into_parts();
    let ctx = match SecurityContext::extract(&mut parts, &state) {
        Ok(ctx) => ctx,
        Err(denied) => return denied.into_response(),
    };

    if let Err(denied) = ctx.require_admin_token() {
        return denied.into_response();
    }
    if let Err(denied) = ctx
        .authorize(Action::Admin, Resource::Cluster)
        .await
    {
        return denied.into_response();
    }

    next.run(axum::extract::Request::from_parts(parts, body))
        .await
}

/// Compare two byte strings without short-circuiting on the first mismatch,
/// so the comparison time doesn't leak how many prefix bytes matched.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b)
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn constant_time_eq_semantics() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secre7"));
        assert!(!constant_time_eq(b"secret", b"secrets"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn authentication_denial_maps_to_http_unauthorized() {
        let response = Denied {
            error: TeoDBError::Unauthorized,
            instance: "/api/v1/query".into(),
        }
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
