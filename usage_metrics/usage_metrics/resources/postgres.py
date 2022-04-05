"""Dagster IO Manager for writing dataframes to postgres."""
import os

import pandas as pd
import sqlalchemy as sa
from dagster import IOManager, MetadataEntry


def get_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine from environment vars."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    return sa.create_engine(f"postgresql://{user}:{password}@{db}:5432")


class DataframePostgresIOManager(IOManager):
    """Dagster IO Manager for writing dataframes to postgres."""

    def handle_output(self, context, obj):
        """Store an output of an op."""
        # name is the name given to the Out that we're storing for
        table_name = context.asset_key.path[-1]
        engine = get_engine()

        with engine.connect() as con:
            obj.to_sql(name=table_name, con=con, if_exists="replace", index=False)

        yield MetadataEntry.int(len(obj), label="number of rows")

    def load_input(self, context):
        """Load an input to an op."""
        # upstream_output.name is the name given to the Out that we're loading for
        table_name = context.upstream_output.asset_key.path[-1]
        engine = get_engine()
        with engine.connect() as con:
            return pd.read_sql_table(table_name, con)
