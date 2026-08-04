//! Canonical mapping from portable TeoDB errors to tonic status values.

use teodb_core::error::{GrpcCode, TeoDBError};
use tonic::{Code, Status};

pub fn status(error: TeoDBError) -> Status {
    let code = match error.grpc_code() {
        GrpcCode::Ok => Code::Ok,
        GrpcCode::Cancelled => Code::Cancelled,
        GrpcCode::InvalidArgument => Code::InvalidArgument,
        GrpcCode::NotFound => Code::NotFound,
        GrpcCode::AlreadyExists => Code::AlreadyExists,
        GrpcCode::PermissionDenied => Code::PermissionDenied,
        GrpcCode::Unauthenticated => Code::Unauthenticated,
        GrpcCode::ResourceExhausted => Code::ResourceExhausted,
        GrpcCode::FailedPrecondition => Code::FailedPrecondition,
        GrpcCode::Aborted => Code::Aborted,
        GrpcCode::Unimplemented => Code::Unimplemented,
        GrpcCode::Internal => Code::Internal,
        GrpcCode::Unavailable => Code::Unavailable,
        GrpcCode::DataLoss => Code::DataLoss,
    };
    let mut status = Status::new(code, error.to_string());
    if let Some(retry_after_ms) = error.retry_after_ms()
        && let Ok(value) = retry_after_ms.to_string().parse()
    {
        status
            .metadata_mut()
            .insert("retry-after-ms", value);
    }
    status
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;
    use teodb_core::ident::TableIdent;

    use super::*;

    #[test]
    fn writer_registry_full_is_failed_precondition() {
        let make_error = || TeoDBError::WriterRegistryFull {
            table: TableIdent::new("analytics", "events"),
            limit: 16,
        };

        let http =
            crate::http::common::problem::problem_from_error(make_error(), "/api/v1/tables/analytics/events/ingest")
                .into_response();
        let grpc = status(make_error());

        assert_eq!(http.status(), axum::http::StatusCode::CONFLICT);
        assert_eq!(grpc.code(), Code::FailedPrecondition);
    }

    #[test]
    fn rate_limit_includes_retry_metadata() {
        let status = status(TeoDBError::RateLimited { retry_after_ms: 1_500 });

        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.metadata().get("retry-after-ms").unwrap(), "1500");
    }
}
