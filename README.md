# PostgreSQL file ingestion

Initial implementation milestone: discover CSV, XLSX, JSON-array, and JSON Lines files; inspect their field names; find a unique eligible PostgreSQL table by required column-name coverage; and persist every match outcome in `ingestion_jobs` (including no-match and ambiguous results).

The current matching policy is deliberately conservative:

- All target columns are required to be present in the source, following the initial requirement. This includes defaulted/generated columns until the client confirms those may be omitted.
- Extra source fields are permitted.
- Matching trims surrounding whitespace, ignores case and internal spaces, and treats `_` and `-` as equivalent by removing them before comparison. For example, `Customer ID`, `customer_id`, and `Customer-ID` match. Duplicate input headers that normalize to the same name are rejected.
- A file matching more than one table is recorded as `ambiguous_match`; the importer does not guess.
- For XLSX, the first worksheet is used for discovery and reading until worksheet rules are confirmed.
- JSON files are expected to be an array of objects; `.jsonl`/`.ndjson` files are also supported.

## Setup

1. Create and activate a Python virtual environment.
2. Install dependencies with `pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` and set `DATABASE_URL`, `INPUT_DIR`, and eligible `SCHEMAS`. The app reads `.env`; `.env` is ignored by Git so local credentials are not committed.
4. Run `python main.py`.

The database role needs permission to read table metadata in the eligible schemas and create the `ingestion_jobs` tracking table. This milestone only discovers and records file/table matches; it does not insert target data yet.

## Next implementation milestones

1. Add durable batch and row-error tables, plus file readers that yield normalized records with source row numbers.
2. Bulk-load batches into raw staging while preserving job, batch, and source-row identity.
3. Validate staged values against the matched table’s current PostgreSQL schema and constraints.
4. Bulk-insert valid rows and bulk-record invalid rows in one transaction per batch; update progress atomically.
5. Add restart/retry handling, worker concurrency, operational status reporting, and tuning controls.
