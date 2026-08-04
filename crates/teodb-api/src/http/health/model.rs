//! Response DTOs for health probes.

use serde::Serialize;

/// JSON response for health checks.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
}
