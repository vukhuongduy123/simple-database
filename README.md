# simple-database

A small, educational key-value database is implemented in Go. The project is intended as a learning exercise and experimental codebase for building a SQL-like parser, a persistent on-disk B-Tree index, and basic locking/coordination for safe concurrent access.

This repository is a work in progress. The primary goals are:

- Implement a SQL-ish parser for simple DDL/DML (work in progress).
- Provide a persistent B-Tree-based index stored on disk (work in progress).
- Add simple locking to enable concurrent access without corruption (work in progress).

Status
------
Overall: Work in progress (WIP).

Key areas and current status:

- SQL parser: WIP. There are parser-related packages present (parser/), but full SQL support and integration with the table layer is incomplete.
- Persistent on-disk B-Tree: WIP. There is an in-repo B-Tree implementation under `internal/table/index/` with many core operations implemented (insert, delete, queries). Persistence logic is scaffolded (Pager types and read/write calls) but the node serialization/deserialization and pager-backed persistence need completion and testing.
- Locking / concurrency: WIP. The codebase contains helper packages and foundation work, but a robust locking scheme (page-level or table-level locks) is not finalized.

What’s implemented (high level)
-------------------------------
- Project layout with clear separation between packages: `internal/`, `table/`, `platform/`, and `cmd/` for the API entrypoint.
- B-Tree algorithm: node structure and in-memory operations (search, insert, split, delete, range queries) implemented in `internal/table/index/btree.go`.
- Supporting internal packages: helper utilities, io readers/parsers, and lower-level data structures (LRU, linked list) that will support caching and pager behavior.

What still needs work (next priorities)
--------------------------------------
1. Pager & persistence
   - Implement node serialization (`MarshalBinary`/`UnmarshalBinary`) and wire it to the `Pager` so nodes are persisted to disk and read back reliably.
   - Implement page allocation, free-list or simple append-only scheme, and ensure PageSize boundaries are respected.
   - Add durable writes (fsync or platform-specific sync) where needed.

2. SQL parser & query planning
   - Finalize the SQL parser for basic CREATE TABLE, INSERT, SELECT, DELETE, and simple WHERE clauses.
   - Implement a basic planner to map parsed SQL to table/index operations.

3. Concurrency & locking
   - Design and implement a locking layer (table-level or page-level) to prevent corruption during concurrent reads/writes.
   - Integrate locks into the pager and table/index operations.

4. Tests and CI
   - Add unit tests for B-Tree operations, pager persistence, and SQL parsing.
   - Add a small CI (GitHub Actions) to run `go test` on push.

5. Tooling & examples
   - Provide usage examples and a small CLI or HTTP API to exercise the DB.

Build & run (local)
-------------------
Requirements:
- Go 1.18+ (module-aware mode)

Build the project:

Generate anltr4 parser:
```aiexclude
antlr4 -Dlanguage=Go -visitor -o .\internal\parser\ .\configs\SelectSqlGrammar.g4
```

For PowerShell on Windows:

```
# from the repository root
go build ./...
```

Run the API server (if implemented) from `cmd/api`:

```
# run the API entrypoint
go run ./cmd/api
```

Notes:
- The code is actively under development: some commands may fail until the B-Tree persistence and parser are implemented.
- The repository includes a `data/` directory with an example data subfolder; however, the format and usage are under development.

Project layout (important paths)
-------------------------------
- cmd/api - API entrypoint (HTTP/CLI server placeholder)
- internal/
  - table/ - table abstractions and index implementations
    - index/ - B-Tree implementation and pager (WIP)
  - platform/ - helper utilities, parsers, and IO helpers
  - parser/ - TLV/encoding helpers and SQL parser pieces
- data/ - default data folder used by the project (example files)

Roadmap / Recommended next steps (short-term)
---------------------------------------------
- Implement `node.UnmarshalBinary` and matching serialization; ensure the `PageSize` fits nodes and keys/values bounded by `maxKeySize`/`maxValSize`.
- Complete Pager Read/Write implementations and create deterministic tests for round-trip persistence of nodes.
- Add basic SQL parser support for `CREATE TABLE`, `INSERT INTO`, and `SELECT` (simple equality and range queries), and map them to table/index operations.
- Add a simple locking mechanism (coarse-grained table lock to start) and tests for concurrent access.
- Add unit tests for all critical components.

Contributing
------------
Contributions are welcome. If you want to help:
1. Open an issue describing the change or task you'd like to take.
2. Fork the repo and make a small, focused change per PR.
3. Add tests for behavior changes where appropriate.

If you want help getting started, a good first task is implementing node serialization and a simple pager persistence test.

Troubleshooting
---------------
- If a component panics or fails, check the logs created by helper/logger and the `data/` folder for any partial files.
- Make sure keys and values stay within the configured `maxKeySize` and `maxValSize`.

License
-------
This project does not currently include a license file. If you plan to publish or accept contributions, consider adding an OSS license (MIT or Apache 2.0 are common choices).

Acknowledgements
----------------
This project is intended as an educational implementation and intentionally keeps things simple to make algorithms and behavior easier to reason about.

Contact / Maintainer
--------------------
Maintainer: project owner (see repo contact info)



