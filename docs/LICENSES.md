# Licensing

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Contributing](../CONTRIBUTING.md), [Code Style](../CODE_STYLE.md)

TeoDB is dual licensed under:

- [Apache License, Version 2.0](../LICENSE)
- [MIT License](../LICENSE-MIT)

The workspace license expression is:

```text
MIT OR Apache-2.0
```

You may use, modify, and distribute the project under either license, at your option.

## Why Dual Licensing

`MIT OR Apache-2.0` is common in the Rust ecosystem. It gives users a permissive MIT option and an Apache-2.0 option with an
explicit patent grant.

## Cargo Manifests

The root workspace manifest sets the license expression. Workspace crates inherit workspace package metadata where applicable.
Internal tooling crates such as the perf suite and test support are not intended for publication.

## SPDX

New source files should use:

```text
SPDX-License-Identifier: MIT OR Apache-2.0
```

Use the comment syntax appropriate for the file type.

## Third-Party Dependencies

Third-party dependencies keep their own licenses. Review dependency license output before publishing binary distributions in
environments with strict compliance requirements.
