# Repository Guidelines

## Project Structure & Module Organization
`src/lib.rs` is the crate entry point for the `partitioner` SQLite extension (`cdylib`). Core logic is split across `src/vtab_interface/` for virtual table behavior, `src/shadow_tables/` for lookup/root/template table management, `src/types/` for parsed SQL and schema types, `src/utils/` for parsing and validation helpers, and `src/error/` for shared error handling. Project docs live in `docs/`, and GitHub Actions workflows live in `.github/workflows/`.

## Build, Test, and Development Commands
Use `cargo build --release` to produce the loadable extension library used by SQLite. Run `cargo test` for the unit tests embedded in the Rust modules. Use `cargo build` during local iteration for faster debug builds. If you are checking formatting before a PR, run `cargo fmt`, and use `cargo clippy` for an extra lint pass when touching core logic.

## Coding Style & Naming Conventions
Follow standard Rust style with 4-space indentation and `rustfmt` defaults. Prefer `snake_case` for functions, modules, and files, and `CamelCase` for structs and enums such as `RootTable` or `LookupTable`. Keep modules focused by behavior: virtual table operations belong under `src/vtab_interface/operations/`, while shadow-table concerns stay under `src/shadow_tables/`. Favor small helper functions over dense SQL-string assembly blocks.

## Testing Guidelines
Tests are currently colocated with implementation files using `#[cfg(test)]` modules, for example in `src/vtab_interface/mod.rs` and `src/shadow_tables/lookup_table.rs`. Add targeted unit tests next to the code you change, and name them after the behavior being verified, such as `creates_lookup_row_for_new_partition`. Run `cargo test` before opening a PR; if partitioning behavior changes, include at least one test covering the SQL path.

## Commit & Pull Request Guidelines
Recent commits use short, descriptive messages in sentence or imperative style, such as `Refactor a bit` or `Add new column to Root table`. Keep commit subjects concise and behavior-focused. Pull requests should explain the user-visible change, note any SQLite or SQL syntax impact, and mention test coverage. Include command output or SQL examples when changing extension behavior or documentation.

## Security & Configuration Tips
This project is experimental and works directly with SQLite shadow tables, so avoid manual edits to generated `_lookup`, `_root`, or `_template` tables in tests unless the case requires it. Validate SQL examples against the extension interface and document any platform-specific behavior when changing release or loading instructions.
