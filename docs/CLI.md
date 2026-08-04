# CLI

Navigation: [README](../README.md) | [Architecture](ARCHITECTURE.md) |
Related: [Configuration](CONFIGURATION.md), [Deployment](DEPLOYMENT.md), [API](API.md)

The server binary is `teodb`.

## Server

```bash
teodb --config deploy/docker/config/standalone.toml
```

Important flags:

| Flag                               | Meaning                                                         |
|------------------------------------|-----------------------------------------------------------------|
| `--config <path>`                  | Load TOML configuration file. Also available as `TEODB_CONFIG`. |
| `--role <ROLE>`                    | Set `standalone`, `data-node`, or `control-plane`.              |
| `--security-mode <MODE>`           | Set `plaintext`, `tls`, or `oauth2`.                            |
| `--rest-bind <addr>`               | Override REST bind address.                                     |
| `--flight-bind <addr>`             | Override Flight SQL bind address.                               |
| `--executor-advertise-host <host>` | Data-node executor host advertised to Ballista.                 |
| `--log-level <level>`              | Override log level.                                             |
| `--log-format <FORMAT>`            | Set `json`, `pretty`, or `compact`.                             |

The CLI is deliberately small. Most configuration belongs in TOML or environment variables.

## Common Source Runs

Standalone against the local Compose infrastructure:

```bash
AWS_ACCESS_KEY_ID=<access-key> \
AWS_SECRET_ACCESS_KEY=<secret-key> \
AWS_REGION=us-east-1 \
AWS_ENDPOINT_URL=http://localhost:19000 \
cargo run --bin teodb -- --config deploy/docker/config/standalone.toml
```

Skip embedded UI build during backend development:

```bash
TEODB_SKIP_UI_BUILD=1 cargo run --bin teodb -- --config deploy/docker/config/standalone.toml
```

## Perf Suite

The performance suite binary is `teodb-perf-suite`.

Commands:

| Command                           | Purpose                                     |
|-----------------------------------|---------------------------------------------|
| `prepare-data --dataset <path>`   | Prepare a dataset under the work directory. |
| `load --dataset <path>`           | Prepare and load a dataset into TeoDB.      |
| `run-suite --bench <path>`        | Run a benchmark scenario.                   |
| `run-flight-bench --bench <path>` | Run Flight SQL specific benchmarks.         |
| `report --input <path>`           | Print a saved benchmark report.             |

Common options include `--http`, `--flight`, `--user`, `--password`, and `--work-dir`.
