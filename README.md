# Simple Go Database

A minimal file-based database engine written in Go, designed for learning and experimentation. The system focuses on core storage and indexing concepts rather than production-ready guarantees.

---

## Features

- **B-Tree Index**  
  Efficient on-disk indexing using a B-Tree structure for fast lookups, inserts, and range scans.

- **File-Based Storage**  
  All data is persisted directly to disk files. No external database required.

- **TLV Encoding (Type–Length–Value)**  
  Data is stored using a hierarchical TLV format with nesting:
  - Page
    - Row
    - Fields (TLV)
- **Fixed Page Size: 4096 bytes**  
  Storage is organized into fixed-size pages to simplify allocation and disk I/O.

- **HTTP Server Interface**  
  The database is exposed over an HTTP API.

- **Custom ANTLR Grammar**  
  Queries are parsed using a custom grammar, enabling a SQL-like. However, unlike SQL, the data is not casting to exact types, thus 
  explicit casting is required.
```aiexclude
SELECT age FROM people WHERE INT32(age) > 21;
```
---

## Work in Progress

The following capabilities are not yet implemented:

- ❌ No transaction support (no ACID guarantees)
- ❌ No reuse of deleted space (files grow monotonically)

---

## How to run
Prerequisites:
- Go 1.25.1+
- Antlr 4.9.2+
1. Run `make build`
2. Cd to output directory and run `./app`

## Storage Layout

### Pages

- Fixed size: **4096 bytes**
- Stored sequentially in a data file
- May contain multiple rows

### Rows

Rows are encoded using TLV and stored inside pages:

Nested TLV structures allow flexible schemas without fixed column layouts.

---

## Indexing

A disk-backed **B-Tree** provides:

- Logarithmic lookup time
- Ordered traversal
- Efficient range queries
- Page-aligned node storage

---

## HTTP API

The database runs as an HTTP server. Typical operations include:

- Insert data
- Query data
- Range scan
- Metadata inspection

Simple API query examples:

Request
```aiexclude
{
  "query": "SELECT * FROM users WHERE age <= INT32(129) LIMIT 10000000"
}
```

Response
```aiexclude
{
  "Rows": [
    {
      "CachePageKey": "users-356",
      "Offset": 361,
      "Size": 55,
      "FullSize": 60,
      "Record": {
        "age": 1,
        "id": 1,
        "record": 1,
        "username": "'This is a user %d'"
      }
    }
  ],
  "AccessType": "Index",
  "RowsInspected": 1,
  "Extra": "Not using page cache"
}
```
## References

- Building a Database Engine