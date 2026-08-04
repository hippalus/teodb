use std::sync::Arc;

use teodb_core::error::TeoDBResult;
use teodb_core::traits::authz::{Action, Authorizer, Principal, Resource};

use crate::observer::{ApiObserver, ApiTransport};

pub struct ApiAuthorization {
    authorizer: Option<Arc<dyn Authorizer>>,
    observer: Arc<dyn ApiObserver>,
}

impl std::fmt::Debug for ApiAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ApiAuthorization")
            .finish_non_exhaustive()
    }
}

impl ApiAuthorization {
    pub fn new(authorizer: Option<Arc<dyn Authorizer>>, observer: Arc<dyn ApiObserver>) -> Self {
        Self { authorizer, observer }
    }

    pub async fn authorize(
        &self,
        transport: ApiTransport,
        principal: &Principal,
        action: Action,
        resource: &Resource,
    ) -> TeoDBResult<()> {
        let result = match &self.authorizer {
            Some(authorizer) => {
                authorizer
                    .authorize(principal, &action, resource)
                    .await
            }
            None => Ok(()),
        };
        self.observer.on_authorization(
            transport,
            if result.is_ok() { "allowed" } else { "denied" },
            &action,
            resource,
        );
        result
    }

    pub fn admission_rejection(&self, transport: ApiTransport, reason: &'static str) {
        self.observer
            .on_admission_rejection(transport, reason);
    }

    pub fn result_bytes(&self, transport: ApiTransport, operation: &'static str, bytes: u64) {
        self.observer
            .on_result_bytes(transport, operation, bytes);
    }

    pub fn write_rejection(&self, reason: &'static str) {
        self.observer.on_write_rejection(reason);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use teodb_core::traits::authz::Principal;

    #[derive(Default)]
    struct CountingAuthorizer {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl Authorizer for CountingAuthorizer {
        async fn authorize(&self, _principal: &Principal, _action: &Action, _resource: &Resource) -> TeoDBResult<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[derive(Default)]
    struct CountingObserver {
        rest: AtomicUsize,
        flight: AtomicUsize,
    }

    impl ApiObserver for CountingObserver {
        fn on_authorization(
            &self,
            transport: ApiTransport,
            outcome: &'static str,
            _action: &Action,
            _resource: &Resource,
        ) {
            assert_eq!(outcome, "allowed");
            match transport {
                ApiTransport::Rest => {
                    self.rest.fetch_add(1, Ordering::Relaxed);
                }
                ApiTransport::Flight => {
                    self.flight.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    #[tokio::test]
    async fn rest_and_flight_share_one_authorization_pass() {
        let authorizer = Arc::new(CountingAuthorizer::default());
        let observer = Arc::new(CountingObserver::default());
        let pass = ApiAuthorization::new(Some(authorizer.clone()), observer.clone());
        let principal = Principal {
            subject: "test-user".into(),
            roles: Vec::new(),
            claims: Default::default(),
        };

        pass.authorize(ApiTransport::Rest, &principal, Action::Query, &Resource::Cluster)
            .await
            .unwrap();
        pass.authorize(ApiTransport::Flight, &principal, Action::Query, &Resource::Cluster)
            .await
            .unwrap();

        assert_eq!(authorizer.calls.load(Ordering::Relaxed), 2);
        assert_eq!(observer.rest.load(Ordering::Relaxed), 1);
        assert_eq!(observer.flight.load(Ordering::Relaxed), 1);
    }
}
