"""Dagster Postgres IOManager."""

import os

import pandas as pd
import sqlalchemy as sa
from dagster import Field, InputContext, IOManager, OutputContext, io_manager

from usage_metrics.models import usage_metrics_metadata


def get_table_name_from_context(context: OutputContext) -> str:
    """Retrieves the table name from the context object."""
    if context.has_asset_key:
        return context.asset_key.to_python_identifier()
    return context.get_identifier()


class PostgresIOManager(IOManager):
    """Manage connection with a Postgres Database."""

    def __init__(
        self,
        user: str,
        password: str,
        db: str,
        ip: str,
        port: str,
        clobber: bool = False,
    ) -> None:
        """Initialize PostgresManager object.

        Args:
            clobber: Clobber and recreate the database if True.
        """
        self.clobber = clobber
        self.engine = sa.create_engine(
            f"postgresql://{user}:{password}@{ip}:{port}/{db}"
        )
        usage_metrics_metadata.create_all(self.engine)

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

    def handle_output(self, context: OutputContext, obj: pd.DataFrame | str):
        """Handle an op or asset output.

        If the output is a dataframe, write it to the database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
            obj: a sql query or dataframe to add to the database.

        Raises:
            Exception: if an asset or op returns an unsupported datatype.
        """
        if isinstance(obj, pd.DataFrame):
            table_name = get_table_name_from_context(context)
            self.append_df_to_table(obj, table_name)
        else:
            raise Exception("PostgresIOManager only supports pandas DataFrames.")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a postgres database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
        """
        table_name = get_table_name_from_context(context)
        engine = self.engine

        with engine.begin() as con:
            try:
                df = pd.read_sql_table(table_name, con)
            except ValueError as err:
                raise ValueError(
                    f"{table_name} not found. Make sure the table is modelled in"
                    "usage_metrics.models.py and regenerate the database."
                ) from err
            if df.empty:
                raise AssertionError(
                    f"The {table_name} table is empty. Materialize "
                    f"the {table_name} asset so it is available in the database."
                )
            return df


@io_manager(
    config_schema={
        "clobber": Field(
            bool,
            description="Clobber and recreate the database if True.",
            default_value=False,
        ),
        "postgres_user": Field(
            str,
            description="Postgres connection string user.",
            default_value=os.environ["POSTGRES_USER"],
        ),
        "postgres_password": Field(
            str,
            description="Postgres connection string password.",
            default_value=os.environ["POSTGRES_PASSWORD"],
        ),
        "postgres_db": Field(
            str,
            description="Postgres connection string database.",
            default_value=os.environ["POSTGRES_DB"],
        ),
        "postgres_ip": Field(
            str,
            description="Postgres connection string ip address.",
            default_value=os.environ["POSTGRES_IP"],
        ),
        "postgres_port": Field(
            str,
            description="Postgres connection string port.",
            default_value=os.environ["POSTGRES_PORT"],
        ),
    }
)
def postgres_manager(init_context) -> PostgresIOManager:
    """Create a PostgresManager dagster resource."""
    clobber = init_context.resource_config["clobber"]
    user = init_context.resource_config["postgres_user"]
    password = init_context.resource_config["postgres_password"]
    db = init_context.resource_config["postgres_db"]
    ip = init_context.resource_config["postgres_ip"]
    port = init_context.resource_config["postgres_port"]
    return PostgresIOManager(
        clobber=clobber,
        user=user,
        password=password,
        db=db,
        ip=ip,
        port=port,
    )
