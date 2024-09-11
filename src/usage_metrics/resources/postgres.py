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
        user: str = os.environ["POSTGRES_USER"],
        password: str = os.environ["POSTGRES_PASSWORD"],
        db: str = os.environ["POSTGRES_DB"],
        ip: str = os.environ["POSTGRES_IP"],
        port: str = os.environ["POSTGRES_PORT"],
    ) -> None:
        """Initialize PostgresManager object.

        Args:
            clobber: Clobber and recreate the database if True.
        """
        self.engine = sa.create_engine(
            f"postgresql://{user}:{password}@{ip}:{port}/{db}"
        )

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
        table_obj = usage_metrics_metadata.tables[table_name]

        # TODO: could also get the insert_ids already in the database
        # and only append the new data.
        with self.engine.begin() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists="append",
                index=False,
                dtype={c.name: c.type for c in table_obj.columns},
            )

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Handle an op or asset output.

        If the output is a dataframe, write it to the database.

        Args:
            context: dagster keyword that provides access output information like asset
                name.
            obj: a dataframe to add to the database.

        Raises:
            Exception: if an asset or op returns an unsupported datatype.
        """
        if isinstance(obj, pd.DataFrame):
            # If a table has a partition key, create a partition_key column
            # to enable subsetting a partition when reading out of SQLite.
            if context.has_partition_key:
                obj["partition_key"] = context.partition_key
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
        table_obj = usage_metrics_metadata.tables[table_name]
        engine = self.engine

        with engine.begin() as con:
            try:
                if context.has_partition_key:
                    query = "SELECT * FROM ? WHERE partition_key = ?"
                else:
                    query = table_name
                df = pd.read_sql(
                    query,
                    con,
                    params=[table_name, context.partition_key],
                    parse_dates=[
                        col.name
                        for col in table_obj.columns
                        if str(col.type) == "TIMESTAMP"
                    ],
                )
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
    user = init_context.resource_config["postgres_user"]
    password = init_context.resource_config["postgres_password"]
    db = init_context.resource_config["postgres_db"]
    ip = init_context.resource_config["postgres_ip"]
    port = init_context.resource_config["postgres_port"]
    return PostgresIOManager(
        user=user,
        password=password,
        db=db,
        ip=ip,
        port=port,
    )
