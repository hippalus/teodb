# Security Policy

Navigation: [README](README.md) | [Architecture](docs/ARCHITECTURE.md) | Related: [Configuration](docs/CONFIGURATION.md), [Network Protocols](docs/NETWORK_PROTOCOL.md), [API](docs/API.md)

TeoDB is early-stage software. It has real security boundaries, but it should be reviewed carefully before exposure to untrusted networks.

## Reporting Vulnerabilities

Please report security issues privately before filing a public issue.

Until a dedicated security advisory channel is configured, contact the repository owner directly through GitHub. Include:

- A description of the issue and affected component.
- Reproduction steps or a proof of concept.
- Whether the issue affects REST, Flight SQL, object storage, catalog access, deployment templates, or local files.
- Suggested mitigations if known.

Do not include secrets, production data, or third-party credentials in reports.

## Supported Versions

The project is pre-1.0. Security fixes target the main branch and the most recent tagged release when tags exist. Older tags may not receive backports.

## Current Security Model

TeoDB supports three configured modes:

- `plaintext`: no TLS requirement and anonymous access is allowed.
- `tls`: TLS transport with optional allow-list authorization.
- `oauth2`: TLS plus JWT validation and optional allow-list authorization.

JWT validation uses configured signing keys and expected issuer/audience settings. The current implementation does not fetch JWKS dynamically.

Admin endpoints and `/metrics` are protected by `security.admin_token` when set. If no admin token is configured, these endpoints are unauthenticated and the server logs a startup warning.

## Deployment Guidance

For local development, the Compose files intentionally use dev credentials. Do not expose those stacks outside a trusted machine.

For shared environments:

- Set `security.admin_token`.
- Use TLS or terminate TLS at a trusted proxy.
- Keep S3 credentials in environment variables or Kubernetes Secrets, not TOML files.
- Restrict Ballista scheduler and executor ports to the TeoDB cluster network.
- Restrict Iceberg REST catalog and object store access to TeoDB and trusted operators.
- Review CORS settings before browser exposure.
- Keep `wal.fsync_on_append = true` for durable deployments.

## Out Of Scope Today

The current project does not provide:

- Multi-tenant isolation.
- Row-level authorization.
- Dynamic JWKS discovery.
- Encrypted local WAL/cache/spill files.
- A database-native replication security model.

These are design areas, not assumptions to rely on.
