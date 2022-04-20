"""Dagster SQLite IOManager."""
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from dagster import resource


class SQLiteManager:
    """Manage connection with SQLite Database."""

    # the locationo of the db?
    # the metadata?
    def __init__(self) -> None:
        """Initialize SQLiteManager object."""
        self.engine = self.get_engine()

    @staticmethod
    def get_engine() -> sa.engine.Engine:
        """Create a sql alchemy engine for sqlite db."""
        sqlite_path = Path(__file__).parents[2] / "data/usage_metrics.db"
        sqlite_path.touch()
        return sa.create_engine("sqlite:///" + str(sqlite_path))

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
