"""Dagster SQLite IOManager."""

import logging
import os
from pathlib import Path

import sqlalchemy as sa
from dagster import Field, Noneable, io_manager

from usage_metrics.resources.sqldatabase import SQLIOManager

logger = logging.getLogger()
logging.basicConfig(level="INFO")


class SQLiteIOManager(SQLIOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database."""

    def __init__(self, db_path=None) -> None:
        """Initialize SQLiteManager object.

        Use sqlite_manager to manage path.

        Args:
            db_path: Path to the sqlite database.
        """
        if db_path is None:
            db_path = os.environ.get("DATA_DIR")
            if db_path is None:
                raise AssertionError(
                    "Need to set a DATA_DIR environment variable to the folder where you want to save the SQLite database."
                )
        db_path = Path(db_path) / "usage_metrics.db"
        logger.info(f"Initializing SQLite IO Manager from: {db_path}")
        if not db_path.exists():
            db_path.parent.mkdir(exist_ok=True)
            db_path.touch()

        engine = sa.create_engine("sqlite:///" + str(db_path))
        self.engine = engine
        self.datetime_column = "DATETIME"


@io_manager(
    config_schema={
        "db_path": Field(
            Noneable(str),
            description="Path to the folder containing the SQLite database. Defaults to $DATA_DIR if None.",
            default_value=None,
        ),
    }
)
def sqlite_manager(init_context) -> SQLiteIOManager:
    """Create a SQLiteManager dagster resource.

    Args:
        db_path: Path to the sqlite database. Default is None,
            which uses DATA_DIR/data/usage_metrics.db
    """
    return SQLiteIOManager(db_path=init_context.resource_config["db_path"])
