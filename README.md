# db-tools v2

A Rust CLI for PostgreSQL data export and cross-database sync, with a built-in config migration tool.

---

## Quick start

```bash
cargo build --release
./target/release/db-tools --help
```

---

## Commands

### `export` — dump table data to files

```bash
db-tools export <FORMAT> -c config.yaml -o ./out
```

Formats: `json` `csv` `xml` `html` `yaml` `insert`

```bash
db-tools export json   -c config.yaml -o ./out
db-tools export insert -c config.yaml -o ./out   # generates SQL INSERT statements
```

### `sync` — diff two databases and emit DML

```bash
db-tools sync -c config.yaml -o dml.sql
db-tools sync -c config.yaml --dry-run    # print SQL without writing file
```

Emits `INSERT`, `UPDATE`, `DELETE` for rows that differ between `source` and `target`.

### `migrate` — config schema tools

```bash
# Upgrade a v1 config to v2 (with coloured diff)
db-tools migrate upgrade -i config.v1.yaml --diff

# Validate a config
db-tools migrate validate -c config.yaml --version v2

# Show what changed between v1 and v2
db-tools migrate changelog
```

---

## Config format (v2)

```yaml
databases:
  source:
    url: "postgresql://user:pass@host:5432/dbname"
  target:           # only needed for sync
    url: "postgresql://user:pass@host:5432/dbname"

tables:
  - name: users
    schema: public  # optional
    order: "id"     # optional sort columns for export
    primary_key:    # required for sync
      - id
    columns:
      - column_name: id
        is_track: true       # read actual DB value
        type: numeric
      - column_name: audit_ts
        is_track: false      # substitute default instead
        type: string
        default: ""
```

See `config.example.v2.yaml` for a full example.

---

## Migrating from v1

If you have a config from the Java version or the original Rust port (v1):

```bash
# Preview changes
db-tools migrate upgrade -i my-config.yaml --diff

# Write converted file
db-tools migrate upgrade -i my-config.yaml -o my-config.v2.yaml
```

### What changes

| Field | v1 | v2 |
|---|---|---|
| DB URL key | `jdbcUrl` | `url` |
| URL prefix | `jdbc:postgresql://` | `postgresql://` |
| Credentials | separate `username`/`password` fields | embedded in URL or separate |
| `is_track` | `"true"` / `"false"` string | `true` / `false` boolean |
| `type` | optional string | optional enum (`string`, `numeric`, `bool`) |
| `schema` | not supported | optional per-table |

---

## Design improvements over v1

| Area | v1 | v2 |
|---|---|---|
| DB driver | `postgres` (sync) | `tokio-postgres` + `deadpool-postgres` (async, pooled) |
| Config types | loose strings | typed enums (`TrackMode`, `ColumnType`, `OutputFormat`) |
| Error handling | `anyhow` everywhere | `thiserror` typed errors + `anyhow` at app boundary |
| Logging | `println!` | `tracing` with `-v`/`-vv`/`-vvv` verbosity |
| Progress | none | `indicatif` progress bar for sync |
| PK collision | `|` join (ambiguous) | `\0` join (collision-safe) |
| Sorting | manual `partial_cmp_with` | `PartialOrd` impl on `Value` |
| Date/UUID | treated as text | native `chrono` + `uuid` types |
| Output formats | single `commands.rs` | one file per format in `format/` |
| Migration | n/a | `migrate upgrade` with LCS diff |

---

## Verbosity / logging

```bash
db-tools -v   sync ...   # INFO  — connection info, table counts
db-tools -vv  sync ...   # DEBUG — SQL queries
db-tools -vvv sync ...   # TRACE — everything
```

Or set `RUST_LOG=debug` to override.
