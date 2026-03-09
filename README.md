# db-tools (Rust port of DatabaseDataCommands)

A Rust CLI tool that replicates the two Spring Shell commands from the original Java code:

| Java `@ShellMethod`  | Rust subcommand    |
|----------------------|--------------------|
| `database-meta`      | `database-meta`    |
| `compare-tables`     | `compare-tables`   |

---

## Build

```bash
cargo build --release
# binary at: ./target/release/db-tools
```

---

## Commands

### `database-meta` — Export table data

Reads a PostgreSQL table (respecting tracked/default columns) and writes it to a file.

```bash
db-tools database-meta <FORMAT> -c config.yaml -o ./out
```

**Formats:** `json` (default), `csv`, `xml`, `html`, `insert` (SQL INSERTs), `yaml`

### `compare-tables` — Diff two databases and emit DML

Compares source vs target databases table-by-table and writes `dml.sql` with INSERT/UPDATE/DELETE statements.

```bash
db-tools compare-tables -c config.yaml
```

---

## Config file format

See `config.example.yaml` for a full example.

```yaml
databases:
  source:
    jdbcUrl: "jdbc:postgresql://host:5432/dbname"
    username: "user"
    password: "pass"
  target:                          # only needed for compare-tables
    jdbcUrl: "jdbc:postgresql://host:5432/dbname"
    username: "user"
    password: "pass"

tables:
  - name: my_table
    order: "col1, col2"            # optional; sort order for export
    primary_key: [col1]            # required for compare-tables
    columns:
      - column_name: col1
        is_track: "true"           # "true" = read from DB
        type: numeric
      - column_name: audit_col
        is_track: "false"          # "false" = replaced with default
        type: string
        default: "N/A"
```

---

## Key design decisions vs Java original

| Java                                  | Rust                                           |
|---------------------------------------|------------------------------------------------|
| jOOQ DSLContext + `Result<Record>`    | `postgres` crate + custom `DbRow`/`Value`      |
| Spring Shell `@ShellComponent`        | `clap` derive-based CLI                        |
| SnakeYAML                             | `serde_yaml`                                   |
| `Collections.sort` with Comparator   | `Vec::sort_by` with closure                    |
| `Objects.equals` + `StringUtils.difference` | `values_equal()` with `\r` stripping  |
| jOOQ SQL query builder                | Hand-built SQL strings in `commands.rs`        |
| `result.formatCSV/JSON/XML/...`       | Custom formatters in `commands.rs`             |

---

## Dependencies

- [`clap`](https://docs.rs/clap) — CLI parsing
- [`postgres`](https://docs.rs/postgres) — PostgreSQL driver
- [`serde`](https://docs.rs/serde) + [`serde_yaml`](https://docs.rs/serde_yaml) + [`serde_json`](https://docs.rs/serde_json) — serialization
- [`csv`](https://docs.rs/csv) — CSV writing
- [`anyhow`](https://docs.rs/anyhow) — error handling
