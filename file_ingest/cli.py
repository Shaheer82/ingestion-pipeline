import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
import json
import sys

from .config import Settings
from .database import connect, ensure_tracking_schema, find_job, load_target_tables, mark_job_failed, record_job
from .ingestion import process_file
from .matching import match_target
from .models import MatchResult
from .readers import discover_files, read_headers


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
    parser = argparse.ArgumentParser(description="Ingest CSV and flat JSON files into matched PostgreSQL tables")
    parser.add_argument("--input-dir", help="Override INPUT_DIR")
    parser.add_argument("--schemas", help="Comma-separated eligible PostgreSQL schemas")
    args = parser.parse_args()

    try:
        settings = Settings.from_environment()
        input_dir = args.input_dir or settings.input_dir
        schemas = tuple(part.strip() for part in args.schemas.split(",") if part.strip()) if args.schemas else settings.schemas
        files = discover_files(input_dir)
        tasks = []
        with closing(connect(settings.jdbc_url, settings.jdbc_driver, settings.jdbc_jar)) as connection:
            ensure_tracking_schema(connection)
            tables = load_target_tables(connection, schemas)
            for file in files:
                previous_job = find_job(connection, file.identity)
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
                if previous_job and previous_job[2] and previous_job[3]:
                    old_target = next(
                        (table for table in tables
                         if table.schema == previous_job[2] and table.name == previous_job[3]),
                        None,
                    )
                    if old_target:
                        match = MatchResult(
                            "table_matched", old_target, (old_target.qualified_name,),
                            "Resuming the target selected for this ingestion job",
                        )
                        job_id = previous_job[0]
                if match is None:
                    try:
                        fields = read_headers(file.path)
                        match = match_target(fields, tables)
                    except Exception as error:
                        match = MatchResult("failed", None, (), str(error))
                batch_size = previous_job[4] if previous_job else settings.batch_size
                job_id = job_id or record_job(connection, file, match, batch_size)
                if match.status == "table_matched":
                    tasks.append((file, job_id, match, batch_size))
                else:
                    print(json.dumps({
                        "job_id": job_id,
                        "file": file.path.name,
                        "status": match.status,
                        "target": match.target.qualified_name if match.target else None,
                        "candidates": list(match.candidates),
                        "details": match.details,
                    }, indent=2))

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
