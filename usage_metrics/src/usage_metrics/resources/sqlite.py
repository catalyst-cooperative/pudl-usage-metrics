"""Dagster SQLite IOManager."""
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from dagster import Field, resource

from usage_metrics.models import usage_metrics_metadata


class SQLiteManager:
    """Manage connection with SQLite Database."""

    def __init__(self, clobber: bool = False) -> None:
        """
        Initialize SQLiteManager object.

        Args:
            clobber: Clobber and recreate the database if True.
        """
        self.engine = self.setup_db(clobber)

    @staticmethod
    def setup_db(clobber: bool = False) -> sa.engine.Engine:
        """
        Create a sqlite db if it doesn't exist and create table schemas.

        Args:
            clobber: Clobber and recreate the database if True.
        Returns:
            engine: SQLAlchemy engine for the sqlite db.
        """
        sqlite_path = Path(__file__).parents[3] / "data/usage_metrics.db"
        engine = sa.create_engine("sqlite:///" + str(sqlite_path))
        if not sqlite_path.exists() or clobber:
            sqlite_path.parent.mkdir(exist_ok=True)
            sqlite_path.touch()
            usage_metrics_metadata.drop_all(engine)
            usage_metrics_metadata.create_all(engine)
        return engine

    def append_df_to_table(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Append a dataframe to a table in the db.

        Args:
            df: The dataframe to append.
            table_name: the name of the database table to append to.
        """
        # TODO: could also get the insert_ids already in the database
        # and only append the new data.
        with self.engine.begin() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists="append",
                index=False,
            )


@resource(
    config_schema={
        "clobber": Field(
            bool,
            description="Clobber and recreate the database if True",
            default_value=False,
        )
    }
)
def sqlite_manager(init_context) -> SQLiteManager:
    """Create a SQLiteManager dagster resource."""
    clobber = init_context.resource_config["clobber"]
    return SQLiteManager(clobber)
