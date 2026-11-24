# Configuration

doewood has two kinds of settings, and each setting has exactly one source
([ADR 0001](adr/0001-architecture.md), section 3):

- **Bootstrap values** come from environment variables, listed below. Every one has a default,
  so `doewood serve` starts with no environment at all.
- **Runtime settings** (S3 sources and credentials, AI providers, policies, schedules) are set
  through the gRPC API and kept in the instance's SQLite store. Secrets are encrypted with the
  master key and are write-only: the API never returns them. (The API arrives in a later step.)

## Bootstrap variables

| variable | default | meaning |
|---|---|---|
| `DOEWOOD_DATA_DIR` | `./doewood-data` | Directory of the store (`doewood.sqlite`) and, in development, the key file. Created if missing. |
| `DOEWOOD_LISTEN` | `127.0.0.1:7443` | `IP:port` of the gRPC API. |
| `DOEWOOD_OPS_LISTEN` | `127.0.0.1:7444` | `IP:port` of the ops endpoints: `/healthz`, `/readyz`, `/metrics`. |
| `DOEWOOD_POSTGRES_URL` | unset: standalone | `postgres://` or `postgresql://` URL of the shared Postgres + pgvector database. |
| `DOEWOOD_MASTER_KEY` | the key file `<data>/master.key` | Base64 of 32 random bytes. Encrypts the secrets in the store. |
| `DOEWOOD_INSTANCE_ID` | the hostname | Name of this instance: 1 to 63 ASCII letters, digits, `-`, `_`, `.`. On Kubernetes, the `StatefulSet` pod name. |

`RUST_LOG` sets the log filter (default `info`), for example `RUST_LOG=doewood=debug,info`.

### Rules

- An empty value counts as unset.
- An invalid value is never fatal: doewood logs a `WARN` naming the variable, uses the default,
  and keeps going. Values that may hold a secret (`DOEWOOD_POSTGRES_URL`,
  `DOEWOOD_MASTER_KEY`) are never echoed, not even in the warning.
- An invalid instance id falls back to the hostname; a hostname that is not a valid id falls
  back to `doewood`.
- **Fatal at start:** a data directory that cannot be created or written, a key file that exists
  but does not hold a valid key (it is never overwritten, since that would lose every secret), a
  master key that differs from the one the store was created with, and an ops address that
  cannot be bound.

### The master key

In production, set `DOEWOOD_MASTER_KEY` from a secret store (a Kubernetes `Secret`):

```sh
openssl rand -base64 32
```

When it is unset (or invalid), doewood uses `<data>/master.key` and generates it on the first
start, with mode 0600 on Unix. This is for development only, and doewood says so with a loud
`WARN` on every start: anyone who can read the data directory can decrypt the secrets, and losing
the file loses them all.

The store keeps a sealed check value, so starting with a different key fails at once, instead
of running with secrets it cannot decrypt.

## Checking the configuration

```sh
doewood config check
```

prints the effective value and origin (`env`, `default`, or `default (env value invalid)`) of
every variable, with secrets redacted, and every warning. It creates nothing.

## Ops endpoints

| path | answers |
|---|---|
| `/healthz` | `200 ok` while the process runs |
| `/readyz` | `200 ready` once the store is open; `503` before that and during shutdown |
| `/metrics` | Prometheus text: `doewood_build_info`, `doewood_ready`, `doewood_start_time_seconds`, `doewood_ops_requests_total` |

On SIGTERM or SIGINT, doewood reports not ready, stops its servers, checkpoints and closes the
store, and exits 0.
