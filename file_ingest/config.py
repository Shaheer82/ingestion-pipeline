from dataclasses import dataclass
import os
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    jdbc_url: str
    jdbc_driver: str
    jdbc_jar: str
    input_dir: Path
    schemas: tuple[str, ...]
    batch_size: int = 1000

    @classmethod
    def from_environment(cls) -> "Settings":
        # Keep local credentials out of source control; .env is gitignored.
        load_dotenv(Path(".env"), override=False)
        jdbc_url = os.environ.get("JDBC_URL", "").strip()
        jdbc_driver = os.environ.get("JDBC_DRIVER", "org.postgresql.Driver").strip()
        jdbc_jar = os.environ.get("JDBC_JAR", "").strip()
        if not jdbc_url:
            raise ValueError("JDBC_URL is required")
        if not jdbc_jar:
            raise ValueError("JDBC_JAR must point to the PostgreSQL JDBC driver JAR")
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
        return cls(jdbc_url, jdbc_driver, jdbc_jar, input_dir, schemas, batch_size)
