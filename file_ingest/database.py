from collections.abc import Sequence
import json

import jaydebeapi

from .models import TargetColumn, TargetTable


TRACKING_DDL = (
"""
CREATE TABLE IF NOT EXISTS ingestion_jobs (
    job_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_identity TEXT NOT NULL UNIQUE,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    target_schema TEXT,
    target_table TEXT,
    status TEXT NOT NULL,
    batch_size INTEGER NOT NULL DEFAULT 1000,
    match_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    details TEXT,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
""",
"""
ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS batch_size INTEGER NOT NULL DEFAULT 1000;
""",
"""
CREATE TABLE IF NOT EXISTS ingestion_batches (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES ingestion_jobs(job_id) ON DELETE CASCADE,
    batch_number INTEGER NOT NULL,
    source_start_row BIGINT NOT NULL,
    source_end_row BIGINT NOT NULL,
    status TEXT NOT NULL,
    rows_staged INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_failed INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_summary TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, batch_number)
);
""",
"""
CREATE TABLE IF NOT EXISTS ingestion_staging_rows (
    job_id BIGINT NOT NULL REFERENCES ingestion_jobs(job_id) ON DELETE CASCADE,
    source_row_number BIGINT NOT NULL,
    raw_record JSONB NOT NULL,
    validation_status TEXT,
    error_category TEXT,
    error_details TEXT,
    validated_at TIMESTAMPTZ,
    PRIMARY KEY (job_id, source_row_number)
);
""",
"""
ALTER TABLE ingestion_staging_rows ADD COLUMN IF NOT EXISTS validation_status TEXT;
""",
"""
ALTER TABLE ingestion_staging_rows ADD COLUMN IF NOT EXISTS error_category TEXT;
""",
"""
ALTER TABLE ingestion_staging_rows ADD COLUMN IF NOT EXISTS error_details TEXT;
""",
"""
ALTER TABLE ingestion_staging_rows ADD COLUMN IF NOT EXISTS validated_at TIMESTAMPTZ;
""",
"""
CREATE TABLE IF NOT EXISTS ingestion_row_errors (
    error_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES ingestion_jobs(job_id) ON DELETE CASCADE,
    batch_number INTEGER NOT NULL,
    source_row_number BIGINT NOT NULL,
    error_category TEXT NOT NULL,
    details TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, source_row_number)
);
""",
)


def connect(jdbc_url: str, jdbc_driver: str, jdbc_jar: str) -> jaydebeapi.Connection:
    """Open a JDBC connection through the configured Java database driver."""
    connection = jaydebeapi.connect(jdbc_driver, jdbc_url, jars=jdbc_jar)
    connection.jconn.setAutoCommit(False)
    return connection


def ensure_tracking_schema(connection: jaydebeapi.Connection) -> None:
    cursor = connection.cursor()
    try:
        for statement in TRACKING_DDL:
            cursor.execute(statement)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def load_target_tables(connection: jaydebeapi.Connection, schemas: Sequence[str]) -> list[TargetTable]:
    if not schemas:
        return []
    schema_placeholders = ", ".join("?" for _ in schemas)
    query = """
        SELECT c.table_schema, c.table_name, c.column_name, c.data_type,
               c.udt_name, c.is_nullable, c.column_default, c.is_generated, c.identity_generation,
               pg_catalog.format_type(a.atttypid, a.atttypmod)
        FROM information_schema.columns AS c
        JOIN information_schema.tables AS t
          ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        JOIN pg_catalog.pg_namespace n ON n.nspname = c.table_schema
        JOIN pg_catalog.pg_class r ON r.relnamespace = n.oid AND r.relname = c.table_name
        JOIN pg_catalog.pg_attribute a ON a.attrelid = r.oid AND a.attname = c.column_name
        WHERE c.table_schema IN ({schema_placeholders})
          AND t.table_type = 'BASE TABLE'
          AND a.attnum > 0 AND NOT a.attisdropped
          AND c.table_name NOT IN (
              'ingestion_jobs', 'ingestion_batches',
              'ingestion_staging_rows', 'ingestion_row_errors'
          )
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
    """.format(schema_placeholders=schema_placeholders)
    cursor = connection.cursor()
    try:
        cursor.execute(query, tuple(schemas))
        rows = cursor.fetchall()
    finally:
        cursor.close()

    grouped: dict[tuple[str, str], list[TargetColumn]] = {}
    for schema, table, name, data_type, udt_name, nullable, default, generated, identity_generation, type_sql in rows:
        grouped.setdefault((schema, table), []).append(
            TargetColumn(
                name=name,
                data_type=data_type,
                udt_name=udt_name,
                nullable=nullable == "YES",
                has_default=default is not None,
                generated=generated != "NEVER",
                type_sql=type_sql,
                identity_always=identity_generation == "ALWAYS",
            )
        )
    return [TargetTable(schema, table, tuple(columns)) for (schema, table), columns in grouped.items()]


def record_job(connection: jaydebeapi.Connection, file, match, batch_size: int = 1000) -> int:
    target_schema = match.target.schema if match.target else None
    target_table = match.target.name if match.target else None
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO ingestion_jobs
                (file_identity, file_name, file_path, file_size_bytes,
                 target_schema, target_table, status, batch_size, match_candidates, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
            ON CONFLICT (file_identity) DO UPDATE SET
                file_name = EXCLUDED.file_name,
                file_path = EXCLUDED.file_path,
                file_size_bytes = EXCLUDED.file_size_bytes,
                target_schema = EXCLUDED.target_schema,
                target_table = EXCLUDED.target_table,
                status = CASE WHEN ingestion_jobs.status IN ('succeeded', 'succeeded_with_errors')
                              THEN ingestion_jobs.status ELSE EXCLUDED.status END,
                batch_size = CASE WHEN ingestion_jobs.status IN ('succeeded', 'succeeded_with_errors')
                                  THEN ingestion_jobs.batch_size ELSE EXCLUDED.batch_size END,
                match_candidates = EXCLUDED.match_candidates,
                details = EXCLUDED.details,
                updated_at = now()
            RETURNING job_id
            """,
            (
                file.identity,
                file.path.name,
                str(file.path.resolve()),
                file.size_bytes,
                target_schema,
                target_table,
                match.status,
                batch_size,
                json.dumps(list(match.candidates)),
                match.details,
            ),
        )
        job_id = cursor.fetchone()[0]
    finally:
        cursor.close()
    connection.commit()
    return job_id


def find_job(connection: jaydebeapi.Connection, file_identity: str):
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT job_id, status, target_schema, target_table, batch_size "
            "FROM ingestion_jobs WHERE file_identity = ?",
            (file_identity,),
        )
        return cursor.fetchone()
    finally:
        cursor.close()


def mark_job_failed(connection: jaydebeapi.Connection, job_id: int, details: str) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute(
            "UPDATE ingestion_jobs SET status = 'failed', details = ?, updated_at = now() WHERE job_id = ?",
            (details[:4000], job_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
