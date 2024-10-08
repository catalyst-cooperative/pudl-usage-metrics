"""Dagster Generic SQL IOManager."""

import pandas as pd
import sqlalchemy as sa
from dagster import InputContext, IOManager, OutputContext

from usage_metrics.helpers import get_table_name_from_context
from usage_metrics.models import usage_metrics_metadata


class SQLIOManager(IOManager):
    """IO Manager that writes and retrieves dataframes from a SQL database.

    You'll need to subclass and implement this to make use of it.
    """

    def __init__(self, **kwargs) -> None:
        """Initialize class. Not implemented.

        Args:
            db_path: Path to the database.
        """
        raise NotImplementedError

    def append_df_to_table(
        self, context: OutputContext, df: pd.DataFrame, table_name: str
    ) -> None:
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

        # We can't filter on context.has_partition_key, since a non-partitioned asset
        # run from a job containing partitioned assets will have
        # context.has_partition_key = True. So for now, add this column only if
        # it is specified in the metadata.

        if context.has_partition_key:
            df["partition_key"] = context.partition_key

        # Get primary key column(s) of dataframe, and check against
        # already-existing data.
        pk_cols = [
            pk_column.name for pk_column in table_obj.primary_key.columns.values()
        ]
        tbl = sa.Table(table_name, sa.MetaData(), autoload_with=self.engine)
        query = sa.select(*[tbl.c[c] for c in pk_cols])  # Only select PK cols

        with self.engine.begin() as conn:
            # TODO: Right now this method does not work consistently for datetime
            # columns, as the datetimes parsed by SQLite and Pandas are not identical in
            # format. Currently, rerunning a partition will raise an error for tables
            # with datetimes as primary keys.

            # Get existing primary keys
            existing_pks = pd.read_sql(sql=query, con=conn)
            i1 = df.set_index(pk_cols).index
            i2 = existing_pks.set_index(pk_cols).index
            # Only update primary keys that aren't in the database
            df_new = df[~i1.isin(i2)]
            if df_new.empty:
                context.log.warn(
                    "All records already loaded, not writing any data. Clobber the database if you want to overwrite this data."
                )
            else:
                df_new.to_sql(
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
            if obj.empty:
                context.log.warning(
                    f"Partition {context.partition_key} has no data, skipping."
                )
            # If a table has a partition key, create a partition_key column
            # to enable subsetting a partition when reading out of SQLite.
            else:
                table_name = get_table_name_from_context(context)
                self.append_df_to_table(context, obj, table_name)
        else:
            raise Exception(
                f"{self.__class__.__name__} only supports pandas DataFrames."
            )

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
                tbl = sa.Table(table_name, sa.MetaData(), autoload_with=engine)
                query = sa.select(tbl)
                if context.has_partition_key:
                    if "partition_key" in table_obj.columns:
                        query = query.where(
                            tbl.c["partition_key"] == context.partition_key
                        )
                    # If you're trying to read in a non-partitioned table,
                    # raise an assertion error that's more targeted than just a key error
                    else:
                        raise AssertionError(
                            f"You're trying to read in a partition of {context.asset_key}, but this table isn't partitioned!"
                        )
                df = pd.read_sql(
                    sql=query,
                    con=con,
                    parse_dates=[
                        col.name
                        for col in table_obj.columns
                        if str(col.type) == self.datetime_column
                    ],
                )
            except ValueError as err:
                raise ValueError(
                    f"{table_name} not found. Make sure the table is modelled in"
                    "usage_metrics.models.py and regenerate the database."
                ) from err
            if df.empty:
                # If table is there but partition is not
                if sa.inspect(engine).has_table(table_name):
                    context.log.warning(
                        f"No data available for partition {context.partition_key}"
                    )
                else:
                    raise AssertionError(
                        f"The {table_name} table is empty. Materialize "
                        f"the {table_name} asset so it is available in the database."
                    )
            return df
