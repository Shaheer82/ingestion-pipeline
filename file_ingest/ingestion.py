"""Batch ingestion, transactional target writes, and restart checkpoints."""

from collections.abc import Iterator
import json
import logging

from .models import DiscoveredFile, MatchResult
from .names import normalize_field_name
from .readers import iter_records, read_headers


logger = logging.getLogger(__name__)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _batches(records, batch_size: int) -> Iterator[list[tuple[int, dict]]]:
    batch = []
    for record in records:
        batch.append(record)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _target_insert_sql(match: MatchResult, headers: list[str]):
    target = match.target
    if target is None:
        raise ValueError("Cannot ingest a file without a unique matched target table")
    input_names = {normalize_field_name(name): name for name in headers}
    insert_columns = [column for column in target.columns if not column.generated]
    if not insert_columns:
        raise ValueError("The matched table has no insertable columns")

    column_sql = ", ".join(_quote_identifier(column.name) for column in insert_columns)
    source_keys = []
    expressions = []
    row_values = []
    for column in insert_columns:
        source_name = input_names[normalize_field_name(column.name)]
        source_keys.append(source_name)
        expressions.append(
            f"CAST((s.raw_record ->> {_quote_literal(source_name)}) AS {column.type_sql})"
        )
        row_values.append(f"CAST(? AS {column.type_sql})")

    table_sql = f"{_quote_identifier(target.schema)}.{_quote_identifier(target.name)}"
    overriding = " OVERRIDING SYSTEM VALUE" if any(column.identity_always for column in insert_columns) else ""
    bulk_sql = (
        f"INSERT INTO {table_sql} ({column_sql}){overriding} "
        f"SELECT {', '.join(expressions)} FROM ingestion_staging_rows AS s "
        "WHERE s.job_id = ? AND s.source_row_number BETWEEN ? AND ? "
        "AND s.validation_status = 'valid' "
        "ORDER BY s.source_row_number"
    )
    row_sql = (
        f"INSERT INTO {table_sql} ({column_sql}){overriding} "
        f"VALUES ({', '.join(row_values)})"
    )
    return bulk_sql, row_sql, source_keys


def _lookup_batch(connection, job_id: int, batch_number: int):
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT status FROM ingestion_batches WHERE job_id = ? AND batch_number = ?",
            (job_id, batch_number),
        )
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        cursor.close()


def _stage_batch(connection, job_id: int, batch_number: int, rows: list[tuple[int, dict]]) -> None:
    start_row, end_row = rows[0][0], rows[-1][0]
    cursor = connection.cursor()
    try:
        cursor.execute(
            """INSERT INTO ingestion_batches
                 (job_id, batch_number, source_start_row, source_end_row, status,
                  rows_staged, started_at, completed_at, updated_at)
               VALUES (?, ?, ?, ?, 'staging', ?, now(), NULL, now())
               ON CONFLICT (job_id, batch_number) DO UPDATE SET
                 source_start_row = EXCLUDED.source_start_row,
                 source_end_row = EXCLUDED.source_end_row,
                 status = 'staging', rows_staged = EXCLUDED.rows_staged,
                 rows_inserted = 0, rows_failed = 0, error_summary = NULL,
                 retry_count = ingestion_batches.retry_count + 1,
                 started_at = now(), completed_at = NULL, updated_at = now()""",
            (job_id, batch_number, start_row, end_row, len(rows)),
        )
        placeholders = ", ".join("(?, ?, ?::jsonb)" for _ in rows)
        values = []
        for source_row, record in rows:
            values.extend((job_id, source_row, json.dumps(record, default=str)))
        cursor.execute(
            "INSERT INTO ingestion_staging_rows (job_id, source_row_number, raw_record) VALUES "
            + placeholders
            + " ON CONFLICT (job_id, source_row_number) DO UPDATE SET "
              "raw_record = EXCLUDED.raw_record, validation_status = NULL, "
              "error_category = NULL, error_details = NULL, validated_at = NULL",
            tuple(values),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def _create_validation_function(connection) -> None:
    cursor = connection.cursor()
    try:
        # Initialize this connection's temporary schema before creating the
        # session-local helper function in pg_temp.
        cursor.execute(
            "CREATE TEMP TABLE IF NOT EXISTS ingest_validation_bootstrap (value INTEGER)"
        )
        cursor.execute(
            """CREATE OR REPLACE FUNCTION pg_temp.ingest_is_valid_cast(input_value TEXT, target_type TEXT)
               RETURNS BOOLEAN LANGUAGE plpgsql AS $$
               BEGIN
                   EXECUTE format('SELECT $1::%s', target_type) USING input_value;
                   RETURN TRUE;
               EXCEPTION WHEN OTHERS THEN
                   RETURN FALSE;
               END;
               $$"""
        )
        cursor.execute("DROP TABLE pg_temp.ingest_validation_bootstrap")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def _validate_batch(connection, job_id: int, batch_number: int, rows, match: MatchResult, headers: list[str]):
    target = match.target
    input_names = {normalize_field_name(name): name for name in headers}
    checks = []
    for column in target.columns:
        if column.generated:
            continue
        source_name = input_names[normalize_field_name(column.name)]
        value_sql = f"s.raw_record ->> {_quote_literal(source_name)}"
        column_label = _quote_literal(column.name)
        type_label = column.type_sql.replace("'", "''")
        if not column.nullable:
            checks.append(
                f"CASE WHEN {value_sql} IS NULL THEN "
                f"'column ' || {column_label} || ' cannot be NULL' END"
            )
        checks.append(
            f"CASE WHEN {value_sql} IS NOT NULL AND NOT "
            f"pg_temp.ingest_is_valid_cast({value_sql}, {_quote_literal(column.type_sql)}) "
            f"THEN 'value for column ' || {column_label} || ' cannot be converted to "
            f"{type_label}' END"
        )

    start_row, end_row = rows[0][0], rows[-1][0]
    cursor = connection.cursor()
    try:
        details = "NULL" if not checks else f"NULLIF(concat_ws('; ', {', '.join(checks)}), '')"
        cursor.execute(
            f"""WITH validation AS (
                    SELECT s.job_id, s.source_row_number, {details} AS error_text
                    FROM ingestion_staging_rows AS s
                    WHERE s.job_id = ? AND s.source_row_number BETWEEN ? AND ?
                )
                UPDATE ingestion_staging_rows AS s
                SET validation_status = CASE WHEN v.error_text IS NULL THEN 'valid' ELSE 'invalid' END,
                    error_category = CASE WHEN v.error_text IS NULL THEN NULL ELSE 'type_or_nullability' END,
                    error_details = v.error_text,
                    validated_at = now()
                FROM validation AS v
                WHERE s.job_id = v.job_id AND s.source_row_number = v.source_row_number""",
            (job_id, start_row, end_row),
        )
        cursor.execute(
            """SELECT source_row_number, validation_status FROM ingestion_staging_rows
               WHERE job_id = ? AND source_row_number BETWEEN ? AND ?""",
            (job_id, start_row, end_row),
        )
        states = dict(cursor.fetchall())
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
    valid_rows = [row for row in rows if states.get(row[0]) == "valid"]
    invalid_count = len(rows) - len(valid_rows)
    return valid_rows, invalid_count


def _write_batch(
    connection,
    job_id: int,
    batch_number: int,
    rows: list[tuple[int, dict]],
    valid_rows: list[tuple[int, dict]],
    invalid_count: int,
    bulk_sql: str,
    row_sql: str,
    source_keys: list[str],
):
    start_row, end_row = rows[0][0], rows[-1][0]
    cursor = connection.cursor()
    inserted = failed = 0
    try:
        cursor.execute(
            "DELETE FROM ingestion_row_errors WHERE job_id = ? AND source_row_number BETWEEN ? AND ?",
            (job_id, start_row, end_row),
        )
        cursor.execute(
            """INSERT INTO ingestion_row_errors
                 (job_id, batch_number, source_row_number, error_category, details)
               SELECT job_id, ?, source_row_number, error_category, error_details
               FROM ingestion_staging_rows
               WHERE job_id = ? AND source_row_number BETWEEN ? AND ?
                 AND validation_status = 'invalid'
               ON CONFLICT (job_id, source_row_number) DO UPDATE SET
                 batch_number = EXCLUDED.batch_number,
                 error_category = EXCLUDED.error_category,
                 details = EXCLUDED.details, created_at = now()""",
            (batch_number, job_id, start_row, end_row),
        )
        failed = invalid_count
        if valid_rows:
            cursor.execute("SAVEPOINT bulk_target_write")
            try:
                cursor.execute(bulk_sql, (job_id, start_row, end_row))
                inserted = len(valid_rows)
                cursor.execute("RELEASE SAVEPOINT bulk_target_write")
                logger.info(
                    "Bulk insert succeeded; Bulk SQL=%s; valid_rows=%d; job id=%d; start row=%d; end row=%d",
                    bulk_sql,
                    len(valid_rows),
                    job_id,
                    start_row,
                    end_row,
                )
            except Exception:
                logger.info(
                    "Bulk insert failed, entered row-by-row insertion; Row SQL=%s; valid_rows=%d",
                    row_sql,
                    len(valid_rows),
                )
                # A database constraint rejected the bulk write. Retry only
                # these otherwise-valid rows to identify the rejected rows.
                cursor.execute("ROLLBACK TO SAVEPOINT bulk_target_write")
                cursor.execute("RELEASE SAVEPOINT bulk_target_write")
                for source_row, record in valid_rows:
                    cursor.execute("SAVEPOINT one_target_row")
                    try:
                        cursor.execute(row_sql, tuple(record.get(key) for key in source_keys))
                        cursor.execute("RELEASE SAVEPOINT one_target_row")
                        cursor.execute(
                            "DELETE FROM ingestion_row_errors WHERE job_id = ? AND source_row_number = ?",
                            (job_id, source_row),
                        )
                        inserted += 1
                    except Exception as error:
                        cursor.execute("ROLLBACK TO SAVEPOINT one_target_row")
                        cursor.execute("RELEASE SAVEPOINT one_target_row")
                        cursor.execute(
                            """INSERT INTO ingestion_row_errors
                                 (job_id, batch_number, source_row_number, error_category, details)
                               VALUES (?, ?, ?, 'target_insert_error', ?)
                               ON CONFLICT (job_id, source_row_number) DO UPDATE SET
                                 batch_number = EXCLUDED.batch_number,
                                 error_category = EXCLUDED.error_category,
                                 details = EXCLUDED.details, created_at = now()""",
                            (job_id, batch_number, source_row, str(error)[:4000]),
                        )
                        failed += 1
        else:
            logger.info(
                "Skipping bulk insert because batch has no valid rows; job id=%d; start row=%d; end row=%d",
                job_id,
                start_row,
                end_row,
            )

        cursor.execute(
            """UPDATE ingestion_batches SET status = ?, rows_inserted = ?, rows_failed = ?,
                 error_summary = ?, completed_at = now(), updated_at = now()
               WHERE job_id = ? AND batch_number = ?""",
            (
                "succeeded_with_errors" if failed else "succeeded",
                inserted,
                failed,
                f"{failed} row(s) failed target validation" if failed else None,
                job_id,
                batch_number,
            ),
        )
        connection.commit()
        return inserted, failed
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def process_file(connection, file: DiscoveredFile, job_id: int, match: MatchResult, batch_size: int):
    headers = read_headers(file.path)
    bulk_sql, row_sql, source_keys = _target_insert_sql(match, headers)
    _create_validation_function(connection)
    cursor = connection.cursor()
    try:
        cursor.execute(
            "UPDATE ingestion_jobs SET status = 'processing', updated_at = now() WHERE job_id = ?",
            (job_id,),
        )
        connection.commit()
    finally:
        cursor.close()

    total_inserted = total_failed = total_batches = 0
    for batch_number, rows in enumerate(_batches(iter_records(file.path), batch_size), start=1):
        if _lookup_batch(connection, job_id, batch_number) in {"succeeded", "succeeded_with_errors"}:
            continue
        _stage_batch(connection, job_id, batch_number, rows)
        valid_rows, invalid_count = _validate_batch(
            connection, job_id, batch_number, rows, match, headers
        )
        inserted, failed = _write_batch(
            connection, job_id, batch_number, rows, valid_rows, invalid_count,
            bulk_sql, row_sql, source_keys
        )
        total_inserted += inserted
        total_failed += failed
        total_batches += 1

    cursor = connection.cursor()
    try:
        cursor.execute("SELECT 1 FROM ingestion_row_errors WHERE job_id = ? LIMIT 1", (job_id,))
        has_errors = cursor.fetchone() is not None
        cursor.execute(
            "UPDATE ingestion_jobs SET status = ?, details = ?, updated_at = now() WHERE job_id = ?",
            (
                "succeeded_with_errors" if has_errors else "succeeded",
                f"Inserted {total_inserted} row(s) during this run; {total_failed} row(s) failed; "
                f"{total_batches} batch(es) processed during this run.",
                job_id,
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
    return total_inserted, total_failed, total_batches
