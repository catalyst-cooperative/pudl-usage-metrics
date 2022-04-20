"""Dagster SQLite IOManager."""
from pathlib import Path

import pandas as pd
import sqlalchemy as sa
from dagster import IOManager, MetadataEntry


def get_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine for sqlite db."""
    sqlite_path = Path(__file__).parents[2] / "data/usage_metrics.db"
    sqlite_path.touch()
    return sa.create_engine("sqlite:///" + str(sqlite_path))


class DataframeSQLiteIOManager(IOManager):
    """Dagster SQLite IOManager."""

    def handle_output(self, context, obj):
        """Handle output."""
        # name is the name given to the Out that we're storing for
        table_name = context.asset_key.path[-1]
        engine = get_engine()

        with engine.connect() as con:
            obj.to_sql(
                name=table_name,
                con=con,
                if_exists="append",
                index=False,
                chunksize=5000,
            )

        yield MetadataEntry.int(len(obj), label="number of rows")

    def load_input(self, context):
        """Load input."""
        # upstream_output.name is the name given to the Out that we're loading for
        table_name = context.upstream_output.asset_key.path[-1]
        engine = get_engine()
        with engine.connect() as con:
            return pd.read_sql_table(table_name, con)
