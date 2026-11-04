# PostgreSQL file ingestion

Initial implementation milestone: discover comma-separated CSV and flat JSON files; inspect their field names; find a unique eligible PostgreSQL table by required column-name coverage; and persist every match outcome in `ingestion_jobs` (including no-match and ambiguous results).

The current matching policy is deliberately conservative:

- All target columns are required to be present in the source, following the initial requirement. This includes defaulted/generated columns until the client confirms those may be omitted.
- Extra source fields are permitted.
- Matching ignores letter case only. Spaces, underscores, hyphens, and other punctuation are preserved. For example, `CUSTOMER_ID` matches `customer_id`, but `Customer ID` does not. Duplicate input headers are rejected when they differ only by letter case.
- A file matching more than one table is recorded as `ambiguous_match`; the importer does not guess.
- CSV files must be comma-separated.
- JSON may be one flat object or an array of flat objects. Nested objects and arrays inside records are rejected. JSON Lines/NDJSON is not supported.

## Setup

1. Create and activate a Python virtual environment with a Java runtime available.
2. Install Python dependencies with `pip install -r requirements.txt`.
3. Download the PostgreSQL JDBC driver JAR and place it in `drivers/` (or another local path).
4. Copy `.env.example` to `.env` and set `JDBC_URL`, `JDBC_JAR`, `INPUT_DIR`, and eligible `SCHEMAS`. `JDBC_DRIVER` defaults to `org.postgresql.Driver`. The app reads `.env`; `.env` is ignored by Git.
5. Run `python main.py`.

Database access uses JDBC through JayDeBeApi and the PostgreSQL JDBC driver. `JDBC_URL` uses the PostgreSQL JDBC format, for example `jdbc:postgresql://localhost:5432/postgres?user=postgres&password=YOUR_PASSWORD`; URL-encode special characters in credentials. `JDBC_JAR` must point to the downloaded PostgreSQL driver JAR.

The database role needs permission to read table metadata in the eligible schemas and create the `ingestion_jobs` tracking table. This milestone only discovers and records file/table matches; it does not insert target data yet.

## Next implementation milestones

1. Add durable batch and row-error tables, plus file readers that yield normalized records with source row numbers.
2. Bulk-load batches into raw staging while preserving job, batch, and source-row identity.
3. Validate staged values against the matched table’s current PostgreSQL schema and constraints.
4. Bulk-insert valid rows and bulk-record invalid rows in one transaction per batch; update progress atomically.
5. Add restart/retry handling, worker concurrency, operational status reporting, and tuning controls.
