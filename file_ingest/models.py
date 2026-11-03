from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TargetColumn:
    name: str
    data_type: str
    udt_name: str
    nullable: bool
    has_default: bool
    generated: bool

    @property
    def required_for_mapping(self) -> bool:
        # Initial policy follows the discussed strict rule: every column must
        # be represented in the source, including default/generated columns.
        # This can be relaxed when the client confirms otherwise.
        return True


@dataclass(frozen=True)
class TargetTable:
    schema: str
    name: str
    columns: tuple[TargetColumn, ...]

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True)
class MatchResult:
    status: str
    target: TargetTable | None
    candidates: tuple[str, ...]
    details: str


@dataclass(frozen=True)
class DiscoveredFile:
    path: Path
    identity: str
    size_bytes: int
