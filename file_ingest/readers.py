import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterator

import ijson
from openpyxl import load_workbook

from .models import DiscoveredFile
from .names import normalize_field_name


SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".json", ".jsonl", ".ndjson"}


def _skip_utf8_bom(source) -> None:
    """Skip an optional UTF-8 byte-order mark without buffering the whole file."""
    prefix = source.read(3)
    if prefix != b"\xef\xbb\xbf":
        source.seek(0)


def discover_files(root: Path) -> list[DiscoveredFile]:
    if not root.exists():
        return []
    result = []
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        # Excel creates these lock files while a workbook is open. They are
        # not source workbooks and are often inaccessible to other processes.
        if path.name.startswith("~$"):
            continue
        if path.suffix.casefold() not in SUPPORTED_SUFFIXES:
            continue
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        result.append(DiscoveredFile(path, digest.hexdigest(), path.stat().st_size))
    return result


def _clean_headers(values: list[Any]) -> list[str]:
    headers = ["" if value is None else str(value).strip() for value in values]
    if not headers or any(not value for value in headers):
        raise ValueError("The file has an empty or missing field name")
    normalized = [normalize_field_name(value) for value in headers]
    if len(set(normalized)) != len(normalized):
        raise ValueError("The file has duplicate field names after normalization")
    return headers


def read_headers(path: Path) -> list[str]:
    suffix = path.suffix.casefold()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            sample = source.read(8192)
            if not sample:
                raise ValueError("The file is empty")
            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel
            source.seek(0)
            return _clean_headers(next(csv.reader(source, dialect)))
    if suffix == ".xlsx":
        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            if not workbook.worksheets:
                raise ValueError("The workbook has no worksheets")
            row = next(workbook.worksheets[0].iter_rows(values_only=True), None)
            if row is None:
                raise ValueError("The first worksheet is empty")
            return _clean_headers(list(row))
        finally:
            workbook.close()
    if suffix in {".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8-sig") as source:
            for line in source:
                if line.strip():
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError("Each JSON Lines record must be an object")
                    return _clean_headers(list(record.keys()))
        raise ValueError("The JSON Lines file has no records")
    if suffix == ".json":
        with path.open("rb") as source:
            _skip_utf8_bom(source)
            first = next(ijson.items(source, "item"), None)
        if not isinstance(first, dict):
            raise ValueError("JSON must be an array of objects for automatic field matching")
        return _clean_headers(list(first.keys()))
    raise ValueError(f"Unsupported file extension: {suffix}")


def iter_records(path: Path) -> Iterator[dict[str, Any]]:
    """Yield records lazily. Header discovery and parser policy are kept separate."""
    suffix = path.suffix.casefold()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            sample = source.read(8192)
            try:
                dialect = csv.Sniffer().sniff(sample) if sample else csv.excel
            except csv.Error:
                dialect = csv.excel
            source.seek(0)
            reader = csv.DictReader(source, dialect=dialect)
            for row in reader:
                yield dict(row)
        return
    if suffix == ".xlsx":
        workbook = load_workbook(path, read_only=True, data_only=True)
        try:
            sheet = workbook.worksheets[0]
            rows = sheet.iter_rows(values_only=True)
            header = _clean_headers(list(next(rows)))
            for values in rows:
                if all(value is None for value in values):
                    continue
                yield dict(zip(header, values))
        finally:
            workbook.close()
        return
    if suffix in {".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8-sig") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise ValueError(f"JSON Lines record {line_number} is not an object")
                yield record
        return
    if suffix == ".json":
        with path.open("rb") as source:
            _skip_utf8_bom(source)
            for record in ijson.items(source, "item"):
                if not isinstance(record, dict):
                    raise ValueError("Each JSON array item must be an object")
                yield record
        return
    raise ValueError(f"Unsupported file extension: {suffix}")
