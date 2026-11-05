import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from dataclasses import replace
import hashlib
import json
import logging
from pathlib import Path
import sys

from .config import Settings
from .database import connect, ensure_tracking_schema, find_job, load_target_tables, mark_job_failed, record_job
from .ingestion import process_file
from .matching import match_target
from .models import MatchResult
from .readers import discover_files, read_headers


def _resolve_requested_table(tables, requested_name: str | None):
    if requested_name is None:
        return None, None
    if "." in requested_name:
        schema_name, table_name = requested_name.split(".", 1)
        candidates = [
            table for table in tables
            if table.schema == schema_name and table.name == table_name
        ]
    else:
        candidates = [table for table in tables if table.name == requested_name]

    if not candidates:
        return None, MatchResult(
            "target_table_not_found",
            None,
            (requested_name,),
            f"Requested target table {requested_name!r} was not found in the eligible schemas",
        )
    if len(candidates) > 1:
        names = tuple(table.qualified_name for table in candidates)
        return None, MatchResult(
            "ambiguous_target_table",
            None,
            names,
            f"Table name {requested_name!r} exists in multiple eligible schemas; use schema.table",
        )
    return candidates[0], None


def _target_scoped_file(file, target_name: str | None, resolved_target):
    if target_name is None:
        return file
    identity_target = resolved_target.qualified_name if resolved_target else target_name
    identity = hashlib.sha256(f"{file.identity}\0{identity_target}".encode("utf-8")).hexdigest()
    return replace(file, identity=identity)


def _process_task(settings: Settings, file, job_id: int, match: MatchResult, batch_size: int):
    output = {
        "job_id": job_id,
        "file": file.path.name,
        "target": match.target.qualified_name if match.target else None,
        "candidates": list(match.candidates),
        "details": match.details,
    }
    try:
        with closing(connect(settings.jdbc_url, settings.jdbc_driver, settings.jdbc_jar)) as connection:
            inserted, failed, batches = process_file(connection, file, job_id, match, batch_size)
            output.update({
                "status": "succeeded_with_errors" if failed else "succeeded",
                "inserted_rows_this_run": inserted,
                "failed_rows_this_run": failed,
                "batches_processed_this_run": batches,
            })
    except Exception as error:
        try:
            with closing(connect(settings.jdbc_url, settings.jdbc_driver, settings.jdbc_jar)) as connection:
                mark_job_failed(connection, job_id, str(error))
        except Exception:
            pass
        output.update({"status": "failed", "details": str(error)})
    return output


def main() -> int:
    log_file = Path(__file__).resolve().parents[1] / "ingestion.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    parser = argparse.ArgumentParser(description="Ingest CSV and flat JSON files into matched PostgreSQL tables")
    parser.add_argument(
        "input_path",
        nargs="?",
        help="CSV/JSON file or folder; defaults to INPUT_DIR",
    )
    parser.add_argument(
        "table_name",
        nargs="?",
        help="Optional target table; use schema.table when a name is ambiguous",
    )
    args = parser.parse_args()

    try:
        settings = Settings.from_environment()
        input_path = Path(args.input_path or settings.input_dir).expanduser()
        schemas = settings.schemas
        files = discover_files(input_path)
        tasks = []
        with closing(connect(settings.jdbc_url, settings.jdbc_driver, settings.jdbc_jar)) as connection:
            ensure_tracking_schema(connection)
            tables = load_target_tables(connection, schemas)
            requested_table, table_selection_error = _resolve_requested_table(tables, args.table_name)
            for file in files:
                job_file = _target_scoped_file(file, args.table_name, requested_table)
                previous_job = find_job(connection, job_file.identity)

                logging.info("Previous job data: %s for file identity: %s", previous_job, job_file.identity)

                if previous_job and previous_job[1] in {"succeeded", "succeeded_with_errors"}:
                    print(json.dumps({
                        "job_id": previous_job[0],
                        "file": file.path.name,
                        "status": previous_job[1],
                        "target": f"{previous_job[2]}.{previous_job[3]}" if previous_job[2] else None,
                        "details": "This file content at this path was already processed; skipping it.",
                    }, indent=2))
                    continue

                job_id = None
                match = None
                if table_selection_error:
                    match = table_selection_error
                elif previous_job and previous_job[2] and previous_job[3]:
                    old_target = next(
                        (table for table in tables
                         if table.schema == previous_job[2] and table.name == previous_job[3]),
                        None,
                    )
                    if old_target and (requested_table is None or old_target == requested_table):
                        match = MatchResult(
                            "table_matched", old_target, (old_target.qualified_name,),
                            "Resuming the target selected for this ingestion job",
                        )
                        job_id = previous_job[0]
                if match is None:
                    try:
                        fields = read_headers(file.path)
                        candidate_tables = [requested_table] if requested_table else tables
                        match = match_target(fields, candidate_tables)
                        if requested_table and match.status != "table_matched":
                            match = MatchResult(
                                "no_table_match",
                                None,
                                (requested_table.qualified_name,),
                                f"File fields do not match every column of the requested table "
                                f"{requested_table.qualified_name}. {match.details}",
                            )
                    except Exception as error:
                        match = MatchResult("failed", None, (), str(error))
                batch_size = previous_job[4] if previous_job else settings.batch_size
                job_id = job_id or record_job(connection, job_file, match, batch_size)
                if match.status == "table_matched":
                    tasks.append((job_file, job_id, match, batch_size))
                else:
                    print(json.dumps({
                        "job_id": job_id,
                        "file": file.path.name,
                        "status": match.status,
                        "target": match.target.qualified_name if match.target else None,
                        "candidates": list(match.candidates),
                        "details": match.details,
                    }, indent=2))

        logging.info(
            "Queued ingestion tasks: %s",
            json.dumps(
                [
                    {
                        "file": file.path.name,
                        "file_identity": file.identity,
                        "job_id": job_id,
                        "target": match.target.qualified_name if match.target else None,
                        "match_status": match.status,
                        "batch_size": batch_size,
                    }
                    for file, job_id, match, batch_size in tasks
                ],
                ensure_ascii=False,
            ),
        )
        if tasks:
            with ThreadPoolExecutor(max_workers=min(settings.workers, len(tasks))) as executor:
                futures = [
                    executor.submit(_process_task, settings, file, job_id, match, batch_size)
                    for file, job_id, match, batch_size in tasks
                ]
                for future in as_completed(futures):
                    print(json.dumps(future.result(), indent=2))
        return 0
    except Exception as error:
        print(f"ingestion setup failed: {error}", file=sys.stderr)
        return 1
