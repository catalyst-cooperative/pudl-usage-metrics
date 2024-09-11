"""Dagster SQLite IOManager."""

from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from dagster import Field, InputContext, IOManager, OutputContext, io_manager

from usage_metrics.models import usage_metrics_metadata

SQLITE_PATH = Path(__file__).parents[3] / "data/usage_metrics.db"


def get_table_name_from_context(context: OutputContext) -> str:
    """Retrieves the table name from the context object."""
    if context.has_asset_key:
        return context.asset_key.to_python_identifier()
    return context.get_identifier()


class SQLiteIOManager(IOManager):
    """IO Manager that writes and retrieves dataframes from a SQLite database."""

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

        self.engine = engine
        self.clobber = clobber

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

        if self.clobber:
            usage_metrics_metadata.drop_all(self.engine, tables=[table_obj])

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
            # If a table has a partition key, create a partition_key column
            # to enable subsetting a partition when reading out of SQLite.
            if context.has_partition_key:
                obj["partition_key"] = context.partition_key
            table_name = get_table_name_from_context(context)
            self.append_df_to_table(obj, table_name)
        else:
            raise Exception("SQLiteIOManager only supports pandas DataFrames.")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a dataframe from a sqlite database.

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
                        if str(col.type) == "DATETIME"
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
def sqlite_manager(init_context) -> SQLiteIOManager:
    """Create a SQLiteManager dagster resource."""
    clobber = init_context.resource_config["clobber"]
    db_path = init_context.resource_config["db_path"]
    return SQLiteIOManager(clobber=clobber, db_path=Path(db_path))
