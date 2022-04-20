"""Dagster SQLite IOManager."""
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from dagster import resource

from usage_metrics.models import usage_metrics_metadata


class SQLiteManager:
    """Manage connection with SQLite Database."""

    def __init__(self) -> None:
        """Initialize SQLiteManager object."""
        self.engine = self.setup_db()

    @staticmethod
    def setup_db():
        """Create a sqlite db if it doesn't exist and create table schemas."""
        sqlite_path = Path(__file__).parents[2] / "data/usage_metrics.db"
        engine = sa.create_engine("sqlite:///" + str(sqlite_path))
        if not sqlite_path.exists():
            sqlite_path.touch()
            usage_metrics_metadata.create_all(engine)
        return engine

    def append_df_to_table(self, df: pd.DataFrame, table_name: str) -> None:
        """Append a dataframe to a table in the db."""
        with self.engine.begin() as conn:
            df.to_sql(name=table_name, con=conn, if_exists="append", index=False)

    def clober_db():
        """Clober the db."""
        pass


@resource
def sqlite_manager(init_context):
    """Create a SQLiteManager dagster resource."""
    return SQLiteManager()
