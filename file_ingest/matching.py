from collections.abc import Iterable

from .models import MatchResult, TargetTable
from .names import normalize_field_name


def match_target(source_fields: Iterable[str], tables: Iterable[TargetTable]) -> MatchResult:
    source = {normalize_field_name(field) for field in source_fields}
    candidates: list[TargetTable] = []
    closest: tuple[TargetTable, tuple[str, ...]] | None = None

    for table in tables:
        required = [column for column in table.columns if column.required_for_mapping]
        missing = tuple(
            column.name
            for column in required
            if normalize_field_name(column.name) not in source
        )
        if not missing:
            candidates.append(table)
        elif closest is None or len(missing) < len(closest[1]):
            closest = (table, missing)

    if len(candidates) == 1:
        target = candidates[0]
        return MatchResult("table_matched", target, (target.qualified_name,), "Unique complete column-name match")
    if len(candidates) > 1:
        names = tuple(table.qualified_name for table in candidates)
        return MatchResult("ambiguous_match", None, names, "More than one eligible table has all required columns")
    if closest:
        table, missing = closest
        return MatchResult(
            "no_table_match",
            None,
            (),
            f"No eligible table matched all required columns; closest candidate {table.qualified_name} is missing: {', '.join(missing)}",
        )
    return MatchResult("no_table_match", None, (), "No eligible target tables were found")
