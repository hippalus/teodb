//! Production middleware stack: request IDs, tracing, rate limiting, fallbacks.

pub mod body_limit;
pub mod fallback;
pub mod observability;
pub mod rate_limit;
pub mod request_id;
pub mod traceparent;

pub use body_limit::enforce_body_limit;
pub use fallback::{handle_fallback, handle_method_not_allowed, handle_panic};
pub use observability::access_log;
pub use rate_limit::RateLimitLayer;
pub use request_id::{REQUEST_ID_HEADER, RequestIdLayer};
pub use traceparent::inject_traceparent;
