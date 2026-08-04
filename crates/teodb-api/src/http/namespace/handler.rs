//! Namespace CRUD endpoint handlers.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::http::header;
use axum::response::{IntoResponse, Response};

use teodb_core::problem::Link;
use teodb_core::traits::authz::{Action, Resource};
use teodb_core::validation::{clamp_page_size, validate_identifier};

use crate::http::common::error::ApiError;
use crate::http::common::hateoas::HateoasResponse;
use crate::http::common::pagination::PaginationParams;
use crate::http::common::security::SecurityContext;
use crate::http::state::AppState;

use super::model::*;

/// GET /api/v1/namespaces — List all namespaces (paginated).
pub async fn list_namespaces(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PaginationParams>,
) -> Result<Response, ApiError> {
    let instance = "/api/v1/namespaces";
    let limit = clamp_page_size(params.limit);
    let offset = params.offset.unwrap_or(0);

    let namespaces = state.services.catalog.list_namespaces().await?;
    let total = namespaces.len();
    let page: Vec<String> = namespaces
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect();

    let resp = HateoasResponse::new(NamespaceListResponse {
        namespaces: page,
        total,
        offset,
        limit,
    })
    .with_link(
        "self",
        Link::new(instance)
            .with_method("GET")
            .with_title("List namespaces"),
    )
    .with_link(
        "create",
        Link::new(instance)
            .with_method("POST")
            .with_title("Create namespace"),
    );

    Ok((StatusCode::OK, Json(resp)).into_response())
}

/// POST /api/v1/namespaces — Create a namespace.
pub async fn create_namespace(
    State(state): State<Arc<AppState>>,
    ctx: SecurityContext,
    Json(body): Json<CreateNamespaceRequest>,
) -> Result<Response, ApiError> {
    validate_identifier("namespace", &body.namespace)?;

    ctx.authorize(Action::CreateTable, Resource::Namespace(body.namespace.clone()))
        .await?;

    let props = body.properties.unwrap_or_default();
    state
        .services
        .catalog
        .create_namespace(&body.namespace, props)
        .await?;

    let ns_uri = format!("/api/v1/namespaces/{}", body.namespace);
    let resp = HateoasResponse::new(NamespaceResponse {
        namespace: body.namespace,
    })
    .with_link("self", Link::new(&ns_uri).with_method("GET"))
    .with_link(
        "tables",
        Link::new(format!("{ns_uri}/tables"))
            .with_method("GET")
            .with_title("List tables"),
    );

    let mut response = (StatusCode::CREATED, Json(resp)).into_response();
    if let Ok(loc) = ns_uri.parse() {
        response
            .headers_mut()
            .insert(header::LOCATION, loc);
    }
    Ok(response)
}

/// GET /api/v1/namespaces/{ns} — Get namespace details.
pub async fn get_namespace(State(_state): State<Arc<AppState>>, Path(ns): Path<String>) -> Result<Response, ApiError> {
    let instance = format!("/api/v1/namespaces/{ns}");

    let resp = HateoasResponse::new(NamespaceResponse { namespace: ns.clone() })
        .with_link("self", Link::new(&instance).with_method("GET"))
        .with_link(
            "tables",
            Link::new(format!("{instance}/tables"))
                .with_method("GET")
                .with_title("List tables in namespace"),
        )
        .with_link(
            "parent",
            Link::new("/api/v1/namespaces")
                .with_method("GET")
                .with_title("All namespaces"),
        );

    Ok((StatusCode::OK, Json(resp)).into_response())
}

/// DELETE /api/v1/namespaces/{ns} — Drop a namespace.
pub async fn drop_namespace(
    State(state): State<Arc<AppState>>,
    ctx: SecurityContext,
    Path(ns): Path<String>,
) -> Result<Response, ApiError> {
    ctx.authorize(Action::DropTable, Resource::Namespace(ns.clone()))
        .await?;

    state.services.catalog.drop_namespace(&ns).await?;

    // Evict buffers of tables in the dropped namespace and tombstone them in
    // the WAL so replay doesn't resurrect them.
    for ident in state.services.buffers.tables() {
        if ident.namespace == ns {
            state.services.buffers.remove(&ident);
            state.services.idempotency.evict_table(&ident);
            crate::ddl_effects::append_tombstone(&state.services.wal, &ident).await;
        }
    }

    Ok(StatusCode::NO_CONTENT.into_response())
}
