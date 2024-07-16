"""Dagster SQLite IOManager."""

from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from dagster import Field, resource
from usage_metrics.models import usage_metrics_metadata

SQLITE_PATH = Path(__file__).parents[3] / "data/usage_metrics.db"


class SQLiteManager:
    """Manage connection with SQLite Database."""

    def __init__(self, clobber: bool = False, db_path: Path = SQLITE_PATH) -> None:
        """Initialize SQLiteManager object.

        Args:
            clobber: Clobber and recreate the database if True.
            db_path: Path to the sqlite database. Defaults to
            usage_metrics/data/usage_metrics.db.
        """
        engine = sa.create_engine("sqlite:///" + str(db_path))
        if not db_path.exists():
            db_path.parent.mkdir(exist_ok=True)
            db_path.touch()

        usage_metrics_metadata.create_all(engine)
        self.engine = engine
        self.clobber = clobber

    def get_engine(self) -> sa.engine.Engine:
        """Get SQLAlchemy engine to interact with the db.

        Returns:
            engine: SQLAlchemy engine for the sqlite db.
        """
        return self.engine

    def append_df_to_table(self, df: pd.DataFrame, table_name: str) -> None:
        """Append a dataframe to a table in the db.

        Args:
            df: The dataframe to append.
            table_name: the name of the database table to append to.
        """
        assert (
            table_name in usage_metrics_metadata.tables
        ), f"""{table_name} does not have a database schema defined.
            Create a schema one in usage_metrics.models."""

        if self.clobber:
            table_obj = usage_metrics_metadata.tables[table_name]
            usage_metrics_metadata.drop_all(self.engine, tables=[table_obj])

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
            description="Clobber and recreate the database if True.",
            default_value=False,
        ),
        "db_path": Field(
            str,
            description="Path to the sqlite database.",
            default_value=str(SQLITE_PATH),
        ),
    }
)
def sqlite_manager(init_context) -> SQLiteManager:
    """Create a SQLiteManager dagster resource."""
    clobber = init_context.resource_config["clobber"]
    db_path = init_context.resource_config["db_path"]
    return SQLiteManager(clobber=clobber, db_path=Path(db_path))
