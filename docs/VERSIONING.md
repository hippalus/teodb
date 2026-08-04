# Versioning

Navigation: [README](../README.md) | [Release Process](../RELEASE.md) |
[Changelog](../CHANGELOG.md)

TeoDB uses `MAJOR.MINOR.PATCH` version numbers. The workspace version is in the
root `Cargo.toml`. A release tag must match it.

```text
tag v0.1.0 -> workspace version 0.1.0
```

## Active Development Rule

TeoDB is pre-1.0. It does not promise backward compatibility.

The latest design is the only supported design. A change may replace an old
API, config key, WAL format, object path, or metadata rule. Development data
may need to be removed and created again.

Do not add compatibility branches, old format readers, or migration code by
default. Add them only after the project makes a clear release policy for
stable users.

Release notes must still list breaking changes. This helps developers update
their local setup. It is not a promise to support the old design.

## Dependency Sets

These packages move together:

| Set | Current version |
|-----|-----------------|
| Arrow and Parquet | 58 |
| DataFusion | 54 |
| Ballista | 54 |
| Iceberg Rust crates | 0.10 |

Do not update one member of the Arrow, DataFusion, and Ballista set by itself.
Plans and Arrow data cross process boundaries.

Update the Iceberg crates as one set. Use the public Iceberg API. Do not keep an
old custom path only for compatibility.

## Write Protocol

The current multi-writer protocol is the only accepted write protocol. Startup
checks WAL identity, prepared intents, writer checkpoints, and protocol fields.
Invalid or old state should fail with a clear error.

## Release Notes

Release notes should list:

- Behavior and API changes.
- Config or local data reset needs.
- Known limits.
- Dependency set changes.
- Deployment changes.
