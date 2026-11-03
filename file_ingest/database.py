from collections.abc import Sequence

import psycopg
from psycopg import sql

from .models import TargetColumn, TargetTable


TRACKING_DDL = """
CREATE TABLE IF NOT EXISTS ingestion_jobs (
    job_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_identity TEXT NOT NULL UNIQUE,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    target_schema TEXT,
    target_table TEXT,
    status TEXT NOT NULL,
    match_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    details TEXT,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url)


def ensure_tracking_schema(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(TRACKING_DDL)
    connection.commit()


def load_target_tables(connection: psycopg.Connection, schemas: Sequence[str]) -> list[TargetTable]:
    query = """
        SELECT c.table_schema, c.table_name, c.column_name, c.data_type,
               c.udt_name, c.is_nullable, c.column_default, c.is_generated
        FROM information_schema.columns AS c
        JOIN information_schema.tables AS t
          ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE c.table_schema = ANY(%s)
          AND t.table_type = 'BASE TABLE'
        ORDER BY c.table_schema, c.table_name, c.ordinal_position
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (list(schemas),))
        rows = cursor.fetchall()

    grouped: dict[tuple[str, str], list[TargetColumn]] = {}
    for schema, table, name, data_type, udt_name, nullable, default, generated in rows:
        grouped.setdefault((schema, table), []).append(
            TargetColumn(
                name=name,
                data_type=data_type,
                udt_name=udt_name,
                nullable=nullable == "YES",
                has_default=default is not None,
                generated=generated != "NEVER",
            )
        )
    return [TargetTable(schema, table, tuple(columns)) for (schema, table), columns in grouped.items()]


def record_job(connection: psycopg.Connection, file, match) -> int:
    target_schema = match.target.schema if match.target else None
    target_table = match.target.name if match.target else None
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ingestion_jobs
                (file_identity, file_name, file_path, file_size_bytes,
                 target_schema, target_table, status, match_candidates, details)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (file_identity) DO UPDATE SET
                file_name = EXCLUDED.file_name,
                file_path = EXCLUDED.file_path,
                file_size_bytes = EXCLUDED.file_size_bytes,
                target_schema = EXCLUDED.target_schema,
                target_table = EXCLUDED.target_table,
                status = EXCLUDED.status,
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
                psycopg.types.json.Json(list(match.candidates)),
                match.details,
            ),
        )
        job_id = cursor.fetchone()[0]
    connection.commit()
    return job_id
