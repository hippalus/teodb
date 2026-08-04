//! Authorization for Flight/FlightSQL handlers — parity with REST `check_auth`.

use tonic::Request;
use tonic::Status;

use teodb_core::error::TeoDBError;
use teodb_core::traits::authz::{Action, Principal, Resource};

use crate::admission::{RateScope, principal_key};
use crate::http::AppState;
use crate::observer::ApiTransport;

pub(crate) fn principal_from_request<T>(
    state: &AppState,
    request: &Request<T>,
    scope: RateScope,
) -> Result<Principal, Status> {
    let token = super::trace::extract_bearer_token(request.metadata());
    let peer = request
        .extensions()
        .get::<tonic::transport::server::TcpConnectInfo>()
        .and_then(tonic::transport::server::TcpConnectInfo::remote_addr)
        .map(|address| address.ip())
        .or_else(|| {
            request
                .extensions()
                .get::<std::net::SocketAddr>()
                .map(std::net::SocketAddr::ip)
        })
        .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
    let headers = proxy_headers(request.metadata());
    let client = state.admission.client_ip(peer, &headers);

    let authenticated = state
        .security
        .authenticator
        .authenticate_bearer(ApiTransport::Flight, token.as_deref())
        .map_err(|error| {
            let key = format!("invalid:{client}");
            match state.admission.check_rate(scope, &key) {
                Ok(()) => authentication_status(error),
                Err(retry_after) => rate_status(retry_after),
            }
        })?;
    let key = format!("principal:{}", principal_key(&authenticated.principal.subject));
    if let Err(retry_after) = state.admission.check_rate(scope, &key) {
        state
            .security
            .authorization
            .admission_rejection(ApiTransport::Flight, "rate");
        return Err(rate_status(retry_after));
    }
    Ok(authenticated.principal)
}

fn proxy_headers(metadata: &tonic::metadata::MetadataMap) -> axum::http::HeaderMap {
    let mut headers = axum::http::HeaderMap::new();
    for name in ["forwarded", "x-forwarded-for"] {
        if let Some(value) = metadata
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| axum::http::HeaderValue::from_str(value).ok())
        {
            headers.insert(axum::http::HeaderName::from_static(name), value);
        }
    }
    headers
}

fn rate_status(retry_after: std::time::Duration) -> Status {
    crate::flight::error::status(TeoDBError::RateLimited {
        retry_after_ms: u64::try_from(retry_after.as_millis()).unwrap_or(u64::MAX),
    })
}

fn authentication_status(error: TeoDBError) -> Status {
    match error {
        TeoDBError::Unauthorized => Status::unauthenticated("invalid or missing bearer token"),
        TeoDBError::Unavailable(message) => Status::unavailable(message),
        other => Status::internal(other.to_string()),
    }
}

/// Authorize a Flight operation (I5 invariant) for the given principal.
///
/// Mirrors the REST `check_auth` semantics: in anonymous (plaintext) mode no
/// authorizer is configured and every action is permitted; otherwise the
/// configured authorizer decides for the request's real identity and denials
/// map to `PERMISSION_DENIED`.
pub(crate) async fn authorize(
    state: &AppState,
    principal: &Principal,
    action: Action,
    resource: Resource,
) -> Result<(), Status> {
    state
        .security
        .authorization
        .authorize(ApiTransport::Flight, principal, action, &resource)
        .await
        .map_err(crate::flight::error::status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authentication_failures_map_to_grpc_unauthenticated() {
        let status = authentication_status(TeoDBError::Unauthorized);
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert!(!status.message().contains("token="));
    }
}
