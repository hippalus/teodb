use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::TeoDBResult;
use crate::ident::TableIdent;

/// Actions that can be authorized.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    CreateTable,
    DropTable,
    AlterTable,
    Ingest,
    Query,
    Compact,
    Admin,
}

/// Resources that authorization applies to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Resource {
    Cluster,
    Namespace(String),
    Table(TableIdent),
}

/// An authenticated principal with associated roles and claims.
#[derive(Debug, Clone)]
pub struct Principal {
    /// Subject identifier, e.g. `"user:alice"` or `"service:flusher"`.
    pub subject: String,
    pub roles: Vec<String>,
    pub claims: HashMap<String, String>,
}

/// Authorization trait. Returns `Ok(())` if the action is allowed;
/// returns `TeoDBError::Forbidden` if denied.
#[async_trait]
pub trait Authorizer: Send + Sync + 'static {
    async fn authorize(&self, principal: &Principal, action: &Action, resource: &Resource) -> TeoDBResult<()>;
}
