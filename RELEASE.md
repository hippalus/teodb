# Release Process

Navigation: [README](README.md) | [Architecture](docs/ARCHITECTURE.md) | Related: [Versioning](docs/VERSIONING.md), [Changelog](CHANGELOG.md), [Testing](docs/TESTING.md)

TeoDB releases are tag-driven. The GitHub release workflow validates the tag, runs CI, builds binaries, publishes a container image, packages the Helm chart, generates checksums, and creates the GitHub release.

## Versioning

The release tag must match the workspace version in `Cargo.toml`.

Valid examples:

- `v0.1.0`
- `v0.2.0-alpha.1`

The workflow rejects a tag when `vX.Y.Z` does not match `workspace.package.version`.

## Pre-Release Checklist

Before tagging:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
```

Run frontend checks when the UI or embedded build path changed:

```bash
cd frontend
npm ci
npm run typecheck
npm run test
npm run build
```

Validate deployment artifacts when they changed:

```bash
docker buildx build --check --file deploy/docker/Dockerfile .
helm lint deploy/helm/teodb
```

Update:

- `CHANGELOG.md`
- `Cargo.toml` workspace version
- Helm chart metadata if the chart version is being changed manually
- Any docs affected by behavior changes

## Cutting A Release

Create and push the tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow performs:

- Tag and version validation.
- Reusable CI workflow.
- Release binary builds for Linux, macOS, and Windows targets.
- Multi-architecture container build and push to GHCR.
- Helm chart lint, package, and OCI push.
- SHA-256 checksum generation.
- Optional GitHub attestations when repository variables enable them.
- GitHub release creation with generated notes.

## Artifacts

Release artifacts include:

- `teodb-<version>-<target>` binary archives.
- `SHA256SUMS`.
- GHCR container images.
- OCI-published Helm chart.

## After Release

Verify:

- The GitHub release contains all expected binary archives and checksums.
- The container image exists under `ghcr.io/<owner>/teodb`.
- The Helm chart exists under `oci://ghcr.io/<owner>/charts`.
- The release notes mention any breaking behavior, migration concerns, or known limits.
