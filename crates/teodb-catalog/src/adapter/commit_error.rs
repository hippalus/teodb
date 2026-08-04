//! Classification of Iceberg 0.10 commit failures.

use iceberg::{Error, ErrorKind};

pub(super) enum CommitAttemptError {
    Conflict(Error),
    StateUnknown(Error),
    RetryableBeforeCommit(Error),
    Fatal(Error),
}

pub(super) fn classify(error: Error) -> CommitAttemptError {
    match error.kind() {
        ErrorKind::CatalogCommitConflicts | ErrorKind::PreconditionFailed => CommitAttemptError::Conflict(error),
        ErrorKind::Unexpected if is_pre_dispatch_connect_failure(&error) => {
            CommitAttemptError::RetryableBeforeCommit(error)
        }
        ErrorKind::Unexpected => {
            // Once a connection exists, an Unexpected error can arrive after
            // publication (for example while sending the body or receiving the
            // response). Exact status resolution must own every such failure.
            CommitAttemptError::StateUnknown(error)
        }
        _ if error.retryable() => CommitAttemptError::RetryableBeforeCommit(error),
        _ => CommitAttemptError::Fatal(error),
    }
}

/// Iceberg 0.10 preserves reqwest's typed error as the source of its
/// `Unexpected` wrapper. A connect failure (including DNS resolution and
/// connection refusal) happens before the HTTP request can reach the catalog,
/// so retrying the exact prepared intent cannot duplicate a published append.
fn is_pre_dispatch_connect_failure(error: &Error) -> bool {
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if cause
            .downcast_ref::<reqwest::Error>()
            .is_some_and(reqwest::Error::is_connect)
        {
            return true;
        }
        source = cause.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unexpected_without_typed_pre_dispatch_evidence_is_state_unknown() {
        for error in [
            Error::new(
                ErrorKind::Unexpected,
                "the commit state is unknown after a gateway timeout",
            ),
            Error::new(ErrorKind::Unexpected, "unclassified transport failure").with_retryable(true),
        ] {
            assert!(matches!(classify(error), CommitAttemptError::StateUnknown(_)));
        }
    }

    #[tokio::test]
    async fn connection_refused_is_retryable_before_commit() {
        let source = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post("http://127.0.0.1:0/v1/namespaces/test/tables/events")
            .send()
            .await
            .expect_err("loopback port zero must refuse the connection");
        assert!(source.is_connect());

        let error = Error::from(source);
        assert!(matches!(classify(error), CommitAttemptError::RetryableBeforeCommit(_)));
    }

    #[tokio::test]
    async fn dns_resolution_failure_is_retryable_before_commit() {
        let source = reqwest::Client::builder()
            .no_proxy()
            .resolve_to_addrs("catalog.test", &[])
            .build()
            .unwrap()
            .post("http://catalog.test/v1/namespaces/test/tables/events")
            .send()
            .await
            .expect_err("an empty DNS override must fail before connecting");
        assert!(source.is_connect());

        let error = Error::from(source);
        assert!(matches!(classify(error), CommitAttemptError::RetryableBeforeCommit(_)));
    }

    #[test]
    fn typed_conflict_wins_over_transport_wording() {
        let error = Error::new(ErrorKind::CatalogCommitConflicts, "gateway timeout while conflicting");
        assert!(matches!(classify(error), CommitAttemptError::Conflict(_)));
    }
}
