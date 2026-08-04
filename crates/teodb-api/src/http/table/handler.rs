//! Table CRUD endpoint handlers.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use futures::TryStreamExt;

use teodb_core::error::{TeoDBError, TeoDBResult};
use teodb_core::ident::TableIdent;
use teodb_core::location::{ObjectLocation, ObjectPath};
use teodb_core::problem::Link;
use teodb_core::schema::SchemaDefinition;
use teodb_core::table::PartitionSpecBuilder;
use teodb_core::traits::authz::{Action, Resource};
use teodb_core::validation::{clamp_page_size, validate_identifier};

use crate::http::common::error::ApiError;
use crate::http::common::hateoas::HateoasResponse;
use crate::http::common::pagination::PaginationParams;
use crate::http::common::security::SecurityContext;
use crate::http::state::AppState;
use crate::service::table;

use super::model::*;

/// GET /api/v1/namespaces/{ns}/tables — List tables in a namespace (paginated).
pub async fn list_tables(
    State(state): State<Arc<AppState>>,
    Path(ns): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Response, ApiError> {
    let instance = format!("/api/v1/namespaces/{ns}/tables");
    let limit = clamp_page_size(params.limit);
    let offset = params.offset.unwrap_or(0);

    let tables = state.services.catalog.list_tables(&ns).await?;
    let total = tables.len();
    let page: Vec<TableIdentResponse> = tables
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|t| TableIdentResponse {
            namespace: t.namespace,
            name: t.name,
        })
        .collect();

    let resp = HateoasResponse::new(TableListResponse {
        tables: page,
        total,
        offset,
        limit,
    })
    .with_link(
        "self",
        Link::new(&instance)
            .with_method("GET")
            .with_title("List tables"),
    )
    .with_link(
        "create",
        Link::new(&instance)
            .with_method("POST")
            .with_title("Create table"),
    )
    .with_link(
        "namespace",
        Link::new(format!("/api/v1/namespaces/{ns}"))
            .with_method("GET")
            .with_title("Namespace details"),
    );

    Ok((StatusCode::OK, Json(resp)).into_response())
}

/// POST /api/v1/namespaces/{ns}/tables — Create a table.
pub async fn create_table(
    State(state): State<Arc<AppState>>,
    ctx: SecurityContext,
    Path(ns): Path<String>,
    Json(body): Json<CreateTableRestRequest>,
) -> Result<Response, ApiError> {
    let instance = format!("/api/v1/namespaces/{ns}/tables");
    let table_name = body.name.clone();

    validate_identifier("namespace", &ns)?;
    validate_identifier("table", &table_name)?;
    for col in &body.columns {
        validate_identifier("column", &col.name)?;
    }

    ctx.authorize(Action::CreateTable, Resource::Namespace(ns.clone()))
        .await?;

    // DTO → domain: column type keywords and partition transforms are parsed
    // by the table service, which owns the data-definition vocabulary.
    let columns = body
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            Ok(teodb_core::schema::ColumnMeta {
                id: (i + 1) as i32,
                name: col.name.clone(),
                data_type: table::data_type_from_keyword(&col.data_type)?,
                nullable: col.nullable,
                doc: None,
            })
        })
        .collect::<teodb_core::error::TeoDBResult<Vec<_>>>()?;

    let schema_def = SchemaDefinition {
        schema_id: 0,
        columns,
        identifier_field_ids: vec![],
    };
    let partition_fields: Vec<(String, String)> = body
        .partition_by
        .iter()
        .map(|f| (f.column.clone(), f.transform.clone()))
        .collect();
    let partition_spec = PartitionSpecBuilder::for_schema(&schema_def)
        .fields(table::partition_field_specs(&partition_fields)?)
        .build()?;

    state
        .services
        .ddl
        .create_table(
            TableIdent::new(&ns, &table_name),
            schema_def,
            partition_spec,
            body.properties,
        )
        .await?;

    let table_uri = format!("/api/v1/namespaces/{ns}/tables/{table_name}");
    let resp = HateoasResponse::new(TableIdentResponse {
        namespace: ns.clone(),
        name: table_name,
    })
    .with_link("self", Link::new(&table_uri).with_method("GET"))
    .with_link(
        "ingest",
        Link::new(format!("/api/v1/tables/{ns}/{}", last_segment(&table_uri)))
            .with_method("POST")
            .with_title("Ingest rows"),
    )
    .with_link(
        "parent",
        Link::new(&instance)
            .with_method("GET")
            .with_title("List tables"),
    );

    let mut response = (StatusCode::CREATED, Json(resp)).into_response();
    if let Ok(loc) = table_uri.parse() {
        response
            .headers_mut()
            .insert(header::LOCATION, loc);
    }
    Ok(response)
}

/// GET /api/v1/namespaces/{ns}/tables/{tbl} — Get table metadata.
pub async fn get_table(
    State(state): State<Arc<AppState>>,
    Path((ns, tbl)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let instance = format!("/api/v1/namespaces/{ns}/tables/{tbl}");
    let ident = TableIdent::new(&ns, &tbl);

    let meta = state.services.catalog.load_table(&ident).await?;
    let columns: Vec<ColumnSchemaInfo> = meta
        .current_schema()?
        .columns
        .iter()
        .map(|f| ColumnSchemaInfo {
            field_id: f.id,
            name: f.name.clone(),
            data_type: f.data_type.to_string(),
            nullable: f.nullable,
            comment: f.doc.clone(),
        })
        .collect();

    let resp = HateoasResponse::new(TableMetadataResponse {
        namespace: ns.clone(),
        name: tbl.clone(),
        current_schema_id: meta.current_schema_id,
        current_snapshot_id: meta.current_snapshot_id,
        columns,
        properties: meta.properties.clone(),
    })
    .with_link("self", Link::new(&instance).with_method("GET"))
    .with_link(
        "ingest",
        Link::new(format!("/api/v1/tables/{ns}/{tbl}/ingest"))
            .with_method("POST")
            .with_title("Ingest rows"),
    )
    .with_link(
        "flush",
        Link::new(format!("/api/v1/tables/{ns}/{tbl}/flush"))
            .with_method("POST")
            .with_title("Flush buffer"),
    )
    .with_link(
        "parent",
        Link::new(format!("/api/v1/namespaces/{ns}/tables"))
            .with_method("GET")
            .with_title("List tables"),
    );

    Ok((StatusCode::OK, Json(resp)).into_response())
}

/// DELETE /api/v1/namespaces/{ns}/tables/{tbl} — Drop a table.
pub async fn drop_table(
    State(state): State<Arc<AppState>>,
    ctx: SecurityContext,
    Path((ns, tbl)): Path<(String, String)>,
    Query(params): Query<DropTableParams>,
) -> Result<Response, ApiError> {
    let ident = TableIdent::new(&ns, &tbl);

    ctx.authorize(Action::DropTable, Resource::Table(ident.clone()))
        .await?;

    let table_location = if params.purge {
        Some(
            state
                .services
                .catalog
                .load_table(&ident)
                .await?
                .table_location
                .to_uri(),
        )
    } else {
        None
    };

    state.services.catalog.drop_table(&ident).await?;

    // Discard the cached buffer — its metadata belongs to the dropped table.
    state.services.buffers.remove(&ident);
    state.services.idempotency.evict_table(&ident);
    // Tombstone the WAL so replay doesn't resurrect the dropped table.
    crate::ddl_effects::append_tombstone(&state.services.wal, &ident).await;

    if let Some(location) = table_location {
        purge_table_storage(&state, &location).await?;
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn purge_table_storage(state: &AppState, table_location: &str) -> TeoDBResult<()> {
    let table_location =
        ObjectLocation::parse(table_location).map_err(|error| TeoDBError::Catalog(error.to_string()))?;
    let (storage, root_path) = state
        .services
        .ddl
        .storage_factory
        .resolve(&table_location)
        .await?;
    let prefix = table_prefix_path(&root_path)?;
    let objects = storage
        .list(&prefix)
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    for object in &objects {
        storage.delete(&object.path).await?;
    }

    Ok(())
}

fn table_prefix_path(root_path: &ObjectPath) -> TeoDBResult<ObjectPath> {
    let key = root_path.as_str().trim_matches('/');
    if key.is_empty() {
        return Err(TeoDBError::Config(
            "table location must include a non-empty object prefix before purge".into(),
        ));
    }
    Ok(ObjectPath::new(format!("{key}/")))
}

/// Extract the last path segment from a URI.
fn last_segment(uri: &str) -> &str {
    uri.rsplit('/').next().unwrap_or(uri)
}
