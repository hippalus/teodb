//! TeoDB client library for HTTP REST and Arrow Flight SQL access.

pub mod flight;
pub mod http;

use thiserror::Error;

/// Result type for TeoDB client operations.
pub type Result<T> = std::result::Result<T, ClientError>;

/// TeoDB client error variants.
#[derive(Debug, Error)]
pub enum ClientError {
    /// HTTP transport or serialization errors.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    /// HTTP middleware (retry/tracing) errors.
    #[error("http middleware error: {0}")]
    HttpMiddleware(#[from] reqwest_middleware::Error),
    /// gRPC/Flight transport errors.
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Arrow processing errors.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// Flight SQL errors.
    #[error("flight error: {0}")]
    Flight(#[from] arrow_flight::error::FlightError),
    /// URL parse errors.
    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),
    /// IO errors.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Expected a Flight ticket but none was returned.
    #[error("missing flight ticket")]
    MissingFlightTicket,
    /// Expected a Flight endpoint but none was returned.
    #[error("missing flight endpoint")]
    MissingFlightEndpoint,
    /// Server returned an error response.
    #[error("server error ({status}): {body}")]
    Server { status: u16, body: String },
}
