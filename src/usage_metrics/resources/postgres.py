"""Dagster SQLite IOManager."""
import os

import pandas as pd
import sqlalchemy as sa
from dagster import Field, resource

from usage_metrics.models import usage_metrics_metadata


class PostgresManager:
    """Manage connection with a Postgres Database."""

    def __init__(self, clobber: bool = False) -> None:
        """
        Initialize SQLiteManager object.

        Args:
            clobber: Clobber and recreate the database if True.
        """
        self.engine = self._create_engine()
        if clobber:
            usage_metrics_metadata.drop_all(self.engine)
        usage_metrics_metadata.create_all(self.engine)

    def _create_engine(self) -> sa.engine.Engine:
        """Create a SqlAlchemy engine for the Cloud SQL databse."""
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASS"]
        db = os.environ["POSTGRES_DB"]
        db_ip = os.environ["POSTGRES_IP"]
        return sa.create_engine(f"postgresql://{user}:{password}@{db_ip}:5432/{db}")

    def get_engine(self) -> sa.engine.Engine:
        """
        Get SQLAlchemy engine to interact with the db.

        Returns:
            engine: SQLAlchemy engine for the sqlite db.
        """
        return self.engine

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
            description="Clobber and recreate the database if True.",
            default_value=False,
        ),
    }
)
def postgres_manager(init_context) -> PostgresManager:
    """Create a PostgresManager dagster resource."""
    clobber = init_context.resource_config["clobber"]
    return PostgresManager(clobber=clobber)
