use arrow::datatypes::Schema;
use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use arrow_flight::sql::{
    CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTables, CommandStatementQuery, ProstMessageExt, SqlInfo,
};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use prost::Message;
use tonic::{Response, Status};

/// Build a `FlightInfo` for a known schema, encoding the SQL into a ticket.
pub(super) fn flight_info_for_schema(
    sql: &str,
    schema: &Schema,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    // Reserve TicketStatementQuery for prepared-statement handles; metadata
    // tickets carry their SQL (or synthetic marker) in a CommandStatementQuery
    // so do_get reads them directly instead of probing the prepared cache.
    let ticket_cmd = CommandStatementQuery {
        query: sql.to_string(),
        transaction_id: None,
    };
    let ticket_bytes = ticket_cmd.as_any().encode_to_vec();
    let ticket = Ticket::new(ticket_bytes);

    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|e| Status::internal(format!("schema encoding: {e}")))?
        .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
        .with_descriptor(descriptor);

    Ok(Response::new(info))
}

/// Build a `FlightInfo` for `CommandGetSqlInfo`.
pub(super) fn flight_info_for_sql_info(
    cmd: &CommandGetSqlInfo,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    let sql_info_data = build_sql_info_data()?;

    let ticket_bytes = cmd.as_any().encode_to_vec();
    let ticket = Ticket::new(ticket_bytes);

    let info = FlightInfo::new()
        .try_with_schema(&sql_info_data.schema())
        .map_err(|e| Status::internal(format!("schema encoding: {e}")))?
        .with_endpoint(FlightEndpoint::new().with_ticket(ticket))
        .with_descriptor(descriptor);

    Ok(Response::new(info))
}

/// Build the static `SqlInfoData` with TeoDB server metadata.
pub(super) fn build_sql_info_data() -> Result<arrow_flight::sql::metadata::SqlInfoData, Status> {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "TeoDB");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "58");
    builder.append(SqlInfo::SqlDdlCatalog, true);
    builder.append(SqlInfo::SqlDdlSchema, true);
    builder.append(SqlInfo::SqlDdlTable, true);
    builder.append(SqlInfo::SqlAllTablesAreSelectable, true);
    builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
    builder
        .build()
        .map_err(|e| Status::internal(format!("failed to build SqlInfoData: {e}")))
}

/// Build the SQL string for `CommandGetDbSchemas`.
pub(super) fn build_db_schemas_sql(cmd: &CommandGetDbSchemas) -> String {
    let mut sql =
        "SELECT DISTINCT table_catalog AS catalog_name, table_schema AS db_schema_name FROM information_schema.tables"
            .to_string();
    let mut conditions = Vec::new();
    if let Some(ref catalog) = cmd.catalog {
        conditions.push(format!("table_catalog = '{catalog}'"));
    }
    if let Some(ref pattern) = cmd.db_schema_filter_pattern {
        conditions.push(format!("table_schema LIKE '{pattern}'"));
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY catalog_name, db_schema_name");
    sql
}

/// Build the SQL string for `CommandGetTables`.
pub(super) fn build_get_tables_sql(cmd: &CommandGetTables) -> String {
    let mut sql =
        "SELECT table_catalog AS catalog_name, table_schema AS db_schema_name, table_name, table_type FROM information_schema.tables"
            .to_string();
    let mut conditions = Vec::new();
    if let Some(ref catalog) = cmd.catalog {
        conditions.push(format!("table_catalog = '{catalog}'"));
    }
    if let Some(ref pattern) = cmd.db_schema_filter_pattern {
        conditions.push(format!("table_schema LIKE '{pattern}'"));
    }
    if let Some(ref pattern) = cmd.table_name_filter_pattern {
        conditions.push(format!("table_name LIKE '{pattern}'"));
    }
    if !cmd.table_types.is_empty() {
        let types: Vec<String> = cmd
            .table_types
            .iter()
            .map(|t| format!("'{t}'"))
            .collect();
        conditions.push(format!("table_type IN ({})", types.join(", ")));
    }
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY catalog_name, db_schema_name, table_name");
    sql
}
