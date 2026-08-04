//! Embedded frontend UI served from the binary.
//!
//! The Vite-built `frontend/dist` is embedded at compile time via `rust_embed`.
//! Asset paths in `index.html` are absolute (e.g. `/assets/index-abc123.js`),
//! so the fallback handler must resolve those paths against the embedded files
//! before falling back to `index.html` for SPA client-side routing.

use axum::body::Body;
use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../../frontend/dist"]
struct UiAssets;

/// Fallback handler: tries to serve the exact embedded file, then falls back
/// to `index.html` for client-side routing (SPA).
pub async fn fallback_handler(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    if path.is_empty() {
        serve_file("index.html")
    } else {
        serve_file(path)
    }
}

fn serve_file(path: &str) -> Response {
    if let Some(content) = UiAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return build_response(
            StatusCode::OK,
            mime.as_ref(),
            cache_control(path),
            content.data.to_vec(),
        );
    }

    // SPA fallback: any unknown path gets index.html so vue-router handles it.
    match UiAssets::get("index.html") {
        Some(index) => build_response(
            StatusCode::OK,
            "text/html; charset=utf-8",
            "no-cache",
            index.data.to_vec(),
        ),
        None => build_response(
            StatusCode::NOT_FOUND,
            "text/plain",
            "no-cache",
            b"UI not available".to_vec(),
        ),
    }
}

/// Build an HTTP response. All inputs are valid by construction so this cannot fail.
fn build_response(status: StatusCode, content_type: &str, cache: &str, body: Vec<u8>) -> Response {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, cache)
        .body(Body::from(body))
        .unwrap_or_else(|_| {
            // Only reachable if status/headers are somehow invalid,
            // which cannot happen with our compile-time constants.
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("internal error"))
                .unwrap_or_default()
        })
}

fn cache_control(path: &str) -> &'static str {
    if path.starts_with("assets/") {
        // Vite hashed assets — immutable, cache forever
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_html_is_embedded() {
        let file = UiAssets::get("index.html");
        assert!(file.is_some(), "index.html must be embedded");
        let embedded = file.unwrap();
        let html = std::str::from_utf8(&embedded.data).unwrap();
        assert!(
            html.contains("<div id=\"app\">"),
            "index.html must contain Vue mount point"
        );
        assert!(html.contains("/assets/"), "index.html must reference asset files");
    }

    #[tokio::test]
    async fn metrics_ui_route_falls_back_to_the_spa() {
        let response = fallback_handler(Uri::from_static("/ui/metrics"))
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .unwrap(),
            "text/html; charset=utf-8"
        );
    }

    #[test]
    fn all_referenced_assets_are_embedded() {
        let index = UiAssets::get("index.html").expect("index.html");
        let html = std::str::from_utf8(&index.data).unwrap();

        // Extract all src= and href= paths from index.html
        for attr in ["src=\"/", "href=\"/"] {
            for chunk in html.split(attr).skip(1) {
                if let Some(end) = chunk.find('"') {
                    let path = &chunk[..end];
                    let result = UiAssets::get(path);
                    assert!(
                        result.is_some(),
                        "Asset '{path}' referenced in index.html is not embedded"
                    );
                }
            }
        }
    }

    #[test]
    fn embedded_file_list_is_nonempty() {
        let files: Vec<_> = UiAssets::iter().collect();
        assert!(files.len() > 5, "expected many embedded files, got {}", files.len());
        eprintln!("Embedded {} files:", files.len());
        for f in &files {
            eprintln!("  {f}");
        }
    }
}
