"""Dagster SQLite IOManager."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory

import sqlalchemy as sa
from dagster import io_manager

from usage_metrics.resources.sqldatabase import SQLIOManager


class SQLiteIOManager(SQLIOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database."""

    def __init__(self, db_path: Path) -> None:
        """Initialize SQLiteManager object.

        Use sqlite_manager to manage path.

        Args:
            db_path: Path to the sqlite database.
        """
        engine = sa.create_engine("sqlite:///" + str(db_path))
        if not db_path.exists():
            db_path.parent.mkdir(exist_ok=True)
            db_path.touch()

        self.engine = engine
        self.datetime_column = "DATETIME"


@io_manager()
def sqlite_manager() -> SQLiteIOManager:
    """Create a SQLiteManager dagster resource."""
    with TemporaryDirectory() as td:
        # Determine where to save these files
        if os.environ.get("DATA_DIR"):
            data_dir = Path(os.environ.get("DATA_DIR"), "data")
            if not Path.exists(data_dir):
                Path.mkdir(data_dir)
        else:
            data_dir = Path(td)

    db_path = data_dir / "usage_metrics.db"
    return SQLiteIOManager(db_path=Path(db_path))
