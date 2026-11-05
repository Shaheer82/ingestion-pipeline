import csv
import hashlib
from pathlib import Path
from typing import Any, Iterator

import ijson

from .models import DiscoveredFile
from .names import normalize_field_name


SUPPORTED_SUFFIXES = {".csv", ".json"}
JSON_WHITESPACE = {b" ", b"\t", b"\r", b"\n"}


def _skip_utf8_bom(source) -> None:
    """Skip an optional UTF-8 byte-order mark without buffering the file."""
    prefix = source.read(3)
    if prefix != b"\xef\xbb\xbf":
        source.seek(0)


def _json_root_marker(path: Path) -> bytes:
    """Return the first non-whitespace JSON byte, after an optional UTF-8 BOM."""
    with path.open("rb") as source:
        _skip_utf8_bom(source)
        while marker := source.read(1):
            if marker not in JSON_WHITESPACE:
                return marker
    raise ValueError("The JSON file is empty")


def _clean_headers(values: list[Any]) -> list[str]:
    # Preserve spaces and punctuation: the agreed matching rule ignores case only.
    headers = ["" if value is None else str(value) for value in values]
    if not headers or any(value == "" for value in headers):
        raise ValueError("The file has an empty or missing field name")
    normalized = [normalize_field_name(value) for value in headers]
    if len(set(normalized)) != len(normalized):
        raise ValueError("The file has duplicate field names when compared case-insensitively")
    return headers


def _validate_flat_json_record(record: Any, record_number: int) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ValueError(f"JSON record {record_number} must be an object")
    for field_name, value in record.items():
        if isinstance(value, (dict, list)):
            raise ValueError(
                f"JSON record {record_number}, field {field_name!r}, contains nested data; "
                "only flat records are supported"
            )
    return record


def discover_files(root: Path) -> list[DiscoveredFile]:
    if not root.exists():
        raise FileNotFoundError(f"Input path does not exist: {root}")
    if root.is_file():
        if root.suffix.casefold() not in SUPPORTED_SUFFIXES:
            raise ValueError(
                f"Unsupported file extension: {root.suffix}. Only .csv and .json are supported"
            )
        paths = [root]
    elif root.is_dir():
        paths = sorted(
            candidate
            for candidate in root.rglob("*")
            if candidate.is_file() and candidate.suffix.casefold() in SUPPORTED_SUFFIXES
        )
    else:
        raise ValueError(f"Input path is not a regular file or folder: {root}")

    result = []
    for path in paths:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        # Include the normalized absolute path so identical content in two
        # different files is tracked as two independent ingestion jobs.
        identity_source = f"{path.resolve()}\0{digest.hexdigest()}".encode("utf-8")
        identity = hashlib.sha256(identity_source).hexdigest()
        result.append(DiscoveredFile(path, identity, path.stat().st_size))
    return result


def read_headers(path: Path) -> list[str]:
    suffix = path.suffix.casefold()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source, delimiter=",")
            headers = next(reader, None)
            if headers is None:
                raise ValueError("The CSV file is empty")
            return _clean_headers(headers)

    if suffix == ".json":
        marker = _json_root_marker(path)
        with path.open("rb") as source:
            _skip_utf8_bom(source)
            if marker == b"[":
                first = next(ijson.items(source, "item"), None)
                record = _validate_flat_json_record(first, 1)
            elif marker == b"{":
                record = _validate_flat_json_record(dict(ijson.kvitems(source, "")), 1)
            else:
                raise ValueError("JSON must be a flat object or an array of flat objects")
        return _clean_headers(list(record.keys()))

    raise ValueError(f"Unsupported file extension: {suffix}. Only .csv and .json are supported")


def iter_records(path: Path) -> Iterator[tuple[int, dict[str, Any]]]:
    """Yield CSV rows or flat JSON records incrementally."""
    suffix = path.suffix.casefold()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source, delimiter=",")
            headers = _clean_headers(next(reader, []))
            for row_number, values in enumerate(reader, start=2):
                if not values:
                    continue
                if len(values) != len(headers):
                    raise ValueError(
                        f"CSV row {row_number} has {len(values)} values; expected {len(headers)}"
                    )
                yield row_number, dict(zip(headers, values))
        return

    if suffix == ".json":
        marker = _json_root_marker(path)
        with path.open("rb") as source:
            _skip_utf8_bom(source)
            if marker == b"[":
                for record_number, record in enumerate(ijson.items(source, "item"), start=1):
                    yield record_number, _validate_flat_json_record(record, record_number)
            elif marker == b"{":
                record = dict(ijson.kvitems(source, ""))
                yield 1, _validate_flat_json_record(record, 1)
            else:
                raise ValueError("JSON must be a flat object or an array of flat objects")
        return

    raise ValueError(f"Unsupported file extension: {suffix}. Only .csv and .json are supported")
