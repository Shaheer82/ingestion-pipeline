from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    database_url: str
    input_dir: Path
    schemas: tuple[str, ...]
    batch_size: int = 1000

    @classmethod
    def from_environment(cls) -> "Settings":
        # Keep local credentials out of source control; .env is gitignored.
        load_dotenv(Path(".env"), override=False)
        database_url = os.environ.get("DATABASE_URL", "").strip()
        if not database_url:
            raise ValueError("DATABASE_URL is required")
        input_dir = Path(os.environ.get("INPUT_DIR", "./input")).expanduser()
        schemas = tuple(
            part.strip()
            for part in os.environ.get("SCHEMAS", "public").split(",")
            if part.strip()
        )
        batch_size = int(os.environ.get("BATCH_SIZE", "1000"))
        if batch_size < 1:
            raise ValueError("BATCH_SIZE must be greater than zero")
        if not schemas:
            raise ValueError("SCHEMAS must contain at least one schema")
        return cls(database_url, input_dir, schemas, batch_size)
