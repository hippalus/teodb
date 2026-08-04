//! Arrow Flight SQL client for TeoDB.

use crate::{ClientError, Result};
use arrow::record_batch::RecordBatch;
use arrow_flight::sql::SqlInfo;
use arrow_flight::sql::client::FlightSqlServiceClient;
use futures::TryStreamExt;
use tonic::transport::{Channel, Endpoint};

/// Flight SQL client wrapper for TeoDB.
#[derive(Debug, Clone)]
pub struct FlightClient {
    client: FlightSqlServiceClient<Channel>,
}

impl FlightClient {
    /// Connect to a Flight SQL endpoint (e.g. `http://127.0.0.1:8815`).
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.to_string())?
            .tcp_nodelay(true)
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(300))
            .http2_keep_alive_interval(std::time::Duration::from_secs(30))
            .keep_alive_timeout(std::time::Duration::from_secs(10))
            .connect()
            .await?;
        Ok(Self {
            client: FlightSqlServiceClient::new(channel),
        })
    }

    /// Perform a Flight SQL handshake for authentication.
    pub async fn handshake(&mut self, username: &str, password: &str) -> Result<()> {
        let _ = self.client.handshake(username, password).await?;
        Ok(())
    }

    /// Execute a SQL query and return record batches.
    pub async fn query(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let flight_info = self.client.execute(sql.to_string(), None).await?;
        let endpoint = flight_info
            .endpoint
            .first()
            .ok_or(ClientError::MissingFlightEndpoint)?;
        let ticket = endpoint
            .ticket
            .clone()
            .ok_or(ClientError::MissingFlightTicket)?;
        let stream = self.client.do_get(ticket).await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(batches)
    }

    /// Execute a DML/DDL statement and return the number of affected rows.
    pub async fn execute_update(&mut self, sql: &str) -> Result<i64> {
        let affected = self
            .client
            .execute_update(sql.to_string(), None)
            .await?;
        Ok(affected)
    }

    /// Execute a query via a prepared statement.
    pub async fn query_prepared(&mut self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut prepared = self.client.prepare(sql.to_string(), None).await?;
        let flight_info = prepared.execute().await?;
        self.fetch_first_endpoint(&flight_info).await
    }

    /// Retrieve the list of catalogs from the server.
    pub async fn get_catalogs(&mut self) -> Result<Vec<RecordBatch>> {
        let flight_info = self.client.get_catalogs().await?;
        self.fetch_first_endpoint(&flight_info).await
    }

    /// Retrieve database schemas, optionally filtered by catalog.
    pub async fn get_schemas(
        &mut self,
        catalog: Option<&str>,
        schema_filter: Option<&str>,
    ) -> Result<Vec<RecordBatch>> {
        let flight_info = self
            .client
            .get_db_schemas(arrow_flight::sql::CommandGetDbSchemas {
                catalog: catalog.map(|s| s.to_string()),
                db_schema_filter_pattern: schema_filter.map(|s| s.to_string()),
            })
            .await?;
        self.fetch_first_endpoint(&flight_info).await
    }

    /// Retrieve table metadata, optionally filtered by catalog, schema, and table name.
    pub async fn get_tables(
        &mut self,
        catalog: Option<&str>,
        schema_filter: Option<&str>,
        table_filter: Option<&str>,
        include_schema: bool,
    ) -> Result<Vec<RecordBatch>> {
        let flight_info = self
            .client
            .get_tables(arrow_flight::sql::CommandGetTables {
                catalog: catalog.map(|s| s.to_string()),
                db_schema_filter_pattern: schema_filter.map(|s| s.to_string()),
                table_name_filter_pattern: table_filter.map(|s| s.to_string()),
                table_types: vec![],
                include_schema,
            })
            .await?;
        self.fetch_first_endpoint(&flight_info).await
    }

    /// Retrieve SQL info metadata entries from the server.
    pub async fn get_sql_info(&mut self, info_ids: Vec<SqlInfo>) -> Result<Vec<RecordBatch>> {
        let flight_info = self.client.get_sql_info(info_ids).await?;
        self.fetch_first_endpoint(&flight_info).await
    }

    /// Retrieve table types (TABLE, VIEW, etc.).
    pub async fn get_table_types(&mut self) -> Result<Vec<RecordBatch>> {
        let flight_info = self.client.get_table_types().await?;
        self.fetch_first_endpoint(&flight_info).await
    }

    /// Retrieve primary keys for a table.
    pub async fn get_primary_keys(
        &mut self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<RecordBatch>> {
        let flight_info = self
            .client
            .get_primary_keys(arrow_flight::sql::CommandGetPrimaryKeys {
                catalog: catalog.map(|s| s.to_string()),
                db_schema: schema.map(|s| s.to_string()),
                table: table.to_string(),
            })
            .await?;
        self.fetch_first_endpoint(&flight_info).await
    }

    /// Fetch record batches from the first endpoint of a `FlightInfo`.
    async fn fetch_first_endpoint(&mut self, flight_info: &arrow_flight::FlightInfo) -> Result<Vec<RecordBatch>> {
        let endpoint = flight_info
            .endpoint
            .first()
            .ok_or(ClientError::MissingFlightEndpoint)?;
        let ticket = endpoint
            .ticket
            .clone()
            .ok_or(ClientError::MissingFlightTicket)?;
        let stream = self.client.do_get(ticket).await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(batches)
    }
}
