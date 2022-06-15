"""Dagster SQLite IOManager."""
import os

import pandas as pd
import pg8000
import sqlalchemy as sa
from dagster import Field, resource
from google.cloud.sql.connector import connector

from usage_metrics.models import usage_metrics_metadata


class PostgresManager:
    """Manage connection with a Postgres Database."""

    def __init__(self, clobber: bool = False) -> None:
        """
        Initialize SQLiteManager object.

        Args:
            clobber: Clobber and recreate the database if True.
        """
        self.engine = self._init_connection_engine()
        if clobber:
            usage_metrics_metadata.drop_all(self.engine)
        usage_metrics_metadata.create_all(self.engine)

    def _init_connection_engine(self) -> sa.engine.Engine:
        """Create a SqlAlchemy engine using Cloud SQL connection client."""

        def getconn() -> pg8000.dbapi.Connection:
            conn: pg8000.dbapi.Connection = connector.connect(
                os.environ["POSTGRES_CONNECTION_NAME"],
                "pg8000",
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASS"],
                db=os.environ["POSTGRES_DB"],
                enable_iam_auth=True,
            )
            return conn

        engine = sa.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )
        engine.dialect.description_encoding = None
        return engine

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
