//! Build script: compiles the Vite frontend into `frontend/dist` so the
//! `teodb-server` binary can embed it via `rust_embed`. The embedded folder is
//! read at compile time, so `dist/` must exist before this crate compiles.
//!
//! Behaviour:
//! - Rebuilds the UI when any frontend source is newer than the built output.
//! - Rebuilds when `dist/` is missing — it is gitignored, so a fresh clone or a
//!   manual delete leaves it absent (the bug this guards against: Cargo would
//!   otherwise cache this script and skip it because no *source* changed).
//! - `TEODB_SKIP_UI_BUILD=1` skips the npm build (e.g. CI builds the UI in a
//!   separate job); an empty `dist/` is created so compilation still succeeds.
//! - If `npm` is unavailable, emits a warning and writes a placeholder `dist/`
//!   so the backend still builds (the web console is just unavailable).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

fn main() {
    let frontend = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../frontend");
    let dist = frontend.join("dist");
    let dist_index = dist.join("index.html");

    // Rebuild when the frontend changes.
    for path in [
        "src",
        "public",
        "index.html",
        "package.json",
        "package-lock.json",
        "vite.config.ts",
        "tsconfig.json",
    ] {
        println!("cargo:rerun-if-changed={}", frontend.join(path).display());
    }
    // Re-run when the embedded output goes missing (Cargo treats a missing
    // watched path as changed), so a deleted/never-built `dist/` is rebuilt.
    println!("cargo:rerun-if-changed={}", dist.display());
    println!("cargo:rerun-if-env-changed=TEODB_SKIP_UI_BUILD");

    if std::env::var_os("TEODB_SKIP_UI_BUILD").is_some() {
        ensure_dir(&dist);
        return;
    }

    if !needs_build(&frontend, &dist_index) {
        return;
    }

    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };
    if !command_ok(npm) {
        println!(
            "cargo:warning=npm not found on PATH — embedding a placeholder web console. \
             Install Node.js and rebuild, or run `cd frontend && npm install && npm run build`."
        );
        write_placeholder(&dist);
        return;
    }

    if !frontend.join("node_modules").exists() {
        let install = if frontend.join("package-lock.json").exists() {
            "ci"
        } else {
            "install"
        };
        run(&frontend, npm, &[install]);
    }
    run(&frontend, npm, &["run", "build"]);

    assert!(
        dist_index.exists(),
        "frontend build did not produce {}",
        dist_index.display()
    );
}

/// Build when the output is absent or any watched source is newer than it.
fn needs_build(frontend: &Path, dist_index: &Path) -> bool {
    let Ok(output) = dist_index.metadata().and_then(|m| m.modified()) else {
        return true; // dist/ missing or unreadable
    };
    [
        "src",
        "index.html",
        "package.json",
        "package-lock.json",
        "vite.config.ts",
    ]
    .iter()
    .filter_map(|p| newest_mtime(&frontend.join(p)))
    .any(|source| source > output)
}

/// Newest modification time at or under `path` (recursing into directories).
fn newest_mtime(path: &Path) -> Option<SystemTime> {
    let meta = path.metadata().ok()?;
    if meta.is_file() {
        return meta.modified().ok();
    }
    let mut newest = meta.modified().ok()?;
    for entry in std::fs::read_dir(path).ok()?.flatten() {
        if let Some(t) = newest_mtime(&entry.path()) {
            newest = newest.max(t);
        }
    }
    Some(newest)
}

fn ensure_dir(dir: &Path) {
    std::fs::create_dir_all(dir).ok();
}

fn write_placeholder(dist: &Path) {
    ensure_dir(dist);
    let _ = std::fs::write(
        dist.join("index.html"),
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <title>TeoDB</title></head><body><div id=\"app\"></div>\
         <p>Web console not built. Run <code>cd frontend &amp;&amp; npm install &amp;&amp; npm run build</code> \
         and rebuild, or rebuild with Node.js on PATH.</p></body></html>",
    );
}

fn command_ok(cmd: &str) -> bool {
    Command::new(cmd)
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

fn run(dir: &Path, cmd: &str, args: &[&str]) {
    let status = Command::new(cmd)
        .current_dir(dir)
        .args(args)
        .status()
        .unwrap_or_else(|e| panic!("failed to run `{cmd} {}`: {e}", args.join(" ")));
    assert!(status.success(), "`{cmd} {}` failed", args.join(" "));
}
