"""Dagster SQLite IOManager."""

from pathlib import Path

import sqlalchemy as sa
from dagster import Field, io_manager

from usage_metrics.resources.sqldatabase import SQLIOManager

SQLITE_PATH = Path(__file__).parents[3] / "data/usage_metrics.db"


class SQLiteIOManager(SQLIOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database."""

    def __init__(self, db_path: Path = SQLITE_PATH) -> None:
        """Initialize SQLiteManager object.

        Args:
            db_path: Path to the sqlite database. Defaults to
            usage_metrics/data/usage_metrics.db.
        """
        engine = sa.create_engine("sqlite:///" + str(db_path))
        if not db_path.exists():
            db_path.parent.mkdir(exist_ok=True)
            db_path.touch()

        self.engine = engine
        self.datetime_column = "DATETIME"


@io_manager(
    config_schema={
        "db_path": Field(
            str,
            description="Path to the sqlite database.",
            default_value=str(SQLITE_PATH),
        ),
    }
)
def sqlite_manager(init_context) -> SQLiteIOManager:
    """Create a SQLiteManager dagster resource."""
    db_path = init_context.resource_config["db_path"]
    return SQLiteIOManager(db_path=Path(db_path))
