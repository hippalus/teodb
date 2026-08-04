//! SQL routing for the REST/Flight query surface.
//!
//! DDL statements run through the catalog `DdlExecutor` and apply their
//! buffer/idempotency/WAL side effects here, so the handler only shapes the
//! result. Everything the pre-parser doesn't recognize as DDL is left for the
//! query engine.

use std::sync::Arc;

use teodb_core::error::TeoDBError;
use teodb_query::ddl::{self, DdlResult, SqlStatement};

use crate::service::DdlService;

/// How a submitted SQL string was handled.
pub enum SqlRouting {
    /// A DDL statement executed; its result is ready for response shaping.
    Ddl(DdlResult),
    /// Not DDL (or the pre-parser couldn't classify it) — the caller should
    /// run it on the query engine.
    Engine,
}

impl DdlService {
    /// Classify `sql`; if it is DDL, execute it against the catalog and apply the
    /// buffer/idempotency/WAL side effects, returning the result. Otherwise report
    /// [`SqlRouting::Engine`] so the caller runs it through the query engine.
    ///
    /// A pre-parser failure is not an error: DataFusion has its own parser, so the
    /// statement falls through to the engine just as before.
    pub async fn route_sql(&self, sql: &str) -> Result<SqlRouting, TeoDBError> {
        match ddl::classify_sql(sql) {
            Ok(SqlStatement::Ddl(plan)) => {
                let executor = ddl::DdlExecutor::new(Arc::clone(&self.catalog), self.default_warehouse_uri.to_string())
                    .with_storage_factory(self.storage_factory.clone());
                let result = executor.execute(plan.clone()).await?;
                crate::ddl_effects::apply_post_ddl(&self.buffers, &self.wal, &self.idempotency, &plan, &result).await;
                Ok(SqlRouting::Ddl(result))
            }
            Ok(SqlStatement::Query(_)) => Ok(SqlRouting::Engine),
            Err(e) => {
                tracing::debug!(error = %e, "sqlparser pre-parse failed, falling through to engine");
                Ok(SqlRouting::Engine)
            }
        }
    }
}
