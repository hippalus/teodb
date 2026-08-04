use teodb_core::traits::authz::{Action, Resource};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiTransport {
    Rest,
    Flight,
}

impl ApiTransport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rest => "rest",
            Self::Flight => "flight",
        }
    }
}

pub trait ApiObserver: Send + Sync + 'static {
    fn on_authentication(&self, _transport: ApiTransport, _outcome: &'static str, _reason: &'static str) {}

    fn on_authorization(
        &self,
        _transport: ApiTransport,
        _outcome: &'static str,
        _action: &Action,
        _resource: &Resource,
    ) {
    }

    fn on_result_bytes(&self, _transport: ApiTransport, _operation: &'static str, _bytes: u64) {}

    fn on_admission_rejection(&self, _transport: ApiTransport, _reason: &'static str) {}

    fn on_write_rejection(&self, _reason: &'static str) {}
}

#[derive(Default)]
pub struct NoopApiObserver;

impl ApiObserver for NoopApiObserver {}
