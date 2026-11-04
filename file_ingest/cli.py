import argparse
from contextlib import closing
import json
import sys

from .config import Settings
from .database import connect, ensure_tracking_schema, load_target_tables, record_job
from .matching import match_target
from .models import MatchResult
from .readers import discover_files, read_headers


def main() -> int:
    parser = argparse.ArgumentParser(description="Discover files and match them to PostgreSQL tables")
    parser.add_argument("--input-dir", help="Override INPUT_DIR")
    parser.add_argument("--schemas", help="Comma-separated eligible PostgreSQL schemas")
    args = parser.parse_args()

    try:
        settings = Settings.from_environment()
        input_dir = args.input_dir or settings.input_dir
        schemas = tuple(part.strip() for part in args.schemas.split(",") if part.strip()) if args.schemas else settings.schemas
        files = discover_files(input_dir)
        with closing(connect(settings.jdbc_url, settings.jdbc_driver, settings.jdbc_jar)) as connection:
            ensure_tracking_schema(connection)
            tables = load_target_tables(connection, schemas)
            for file in files:
                try:
                    fields = read_headers(file.path)
                    match = match_target(fields, tables)
                except Exception as error:
                    match = MatchResult("failed", None, (), str(error))
                job_id = record_job(connection, file, match)
                print(json.dumps({
                    "job_id": job_id,
                    "file": file.path.name,
                    "status": match.status,
                    "target": match.target.qualified_name if match.target else None,
                    "candidates": list(match.candidates),
                    "details": match.details,
                }, indent=2))
        return 0
    except Exception as error:
        print(f"ingestion setup failed: {error}", file=sys.stderr)
        return 1
