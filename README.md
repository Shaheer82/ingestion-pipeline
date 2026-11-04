# PostgreSQL file ingestion

The importer discovers comma-separated CSV and flat JSON files, matches each file to a unique eligible PostgreSQL table by its field names, and loads matching files in resumable batches. A target table is selected only when all its columns are present in the input; additional input fields are allowed. No-match and ambiguous outcomes are also recorded in `ingestion_jobs`.

## Matching and input rules

- Column matching ignores letter case only. Spaces, underscores, hyphens, and other punctuation are preserved. For example, `CUSTOMER_ID` matches `customer_id`, but `Customer ID` does not.
- Duplicate input headers are rejected when they differ only by letter case.
- CSV files must be comma-separated.
- JSON may be one flat object or an array of flat objects. Nested objects and arrays inside records are rejected. JSON Lines/NDJSON is not supported.
- Duplicate data rows are loaded as received; no deduplication is performed.

## Setup

1. Create and activate a Python virtual environment with a Java runtime available.
2. Install dependencies with `pip install -r requirements.txt`.
3. Download the PostgreSQL JDBC driver JAR and place it in `drivers/` (or another local path).
4. Copy `.env.example` to `.env` and set `JDBC_URL`, `JDBC_JAR`, `INPUT_DIR`, and eligible `SCHEMAS`. `JDBC_DRIVER` defaults to `org.postgresql.Driver`. The app reads `.env`; `.env` is ignored by Git.
5. Run `python main.py`.

Database access uses JDBC through JayDeBeApi and the PostgreSQL JDBC driver. `JDBC_URL` uses the PostgreSQL JDBC format, for example `jdbc:postgresql://localhost:5432/postgres?user=postgres&password=YOUR_PASSWORD`; URL-encode special characters in credentials. `JDBC_JAR` must point to the downloaded PostgreSQL driver JAR.

Set `BATCH_SIZE` to control rows per batch (default `1000`) and `WORKERS` to control concurrent files (default `4`). Each worker opens its own JDBC connection. The database role needs permission to read table metadata, create the ingestion tracking tables, and insert into eligible target tables.

## Processing and recovery

For each unique table match, records are streamed into `ingestion_staging_rows` with their source row number. PostgreSQL then attempts one set-based insert into the target table for the batch. If that statement fails, it is rolled back to a savepoint and rows are retried individually inside the same transaction. This allows valid rows to be committed while invalid rows and their database error details are saved in `ingestion_row_errors`. Target inserts, row errors, and batch completion status commit together.

The tracking tables are `ingestion_jobs`, `ingestion_batches`, `ingestion_staging_rows`, and `ingestion_row_errors`. `ingestion_batches` records batch status, row counts, and retries. CSV source row numbering starts at 2 to account for the header; JSON record numbering starts at 1.

When the same file at the same absolute path is run again, completed jobs are skipped. Incomplete jobs reuse their stored target and batch size, skip completed batches, and restart at the first incomplete batch. The importer still streams past earlier source rows to reach that batch, but it does not write completed rows again. A changed file has a different content fingerprint and becomes a new job.

## Current limitations

- PostgreSQL enforces type conversions and constraints during insertion. A batch with an invalid row uses the slower per-row savepoint fallback; batches with only valid rows use one set-based insert.
- A malformed CSV row shape or structurally invalid JSON file currently fails the file read. Row-level recovery applies to records that parse successfully but fail PostgreSQL type or constraint checks.
- Completed batches and raw staging data are retained for audit and resume. A cleanup policy can be added later if needed.
- Workers run across files. Batches within one file are processed in order.
