import os
import sqlalchemy as sa
import pandas as pd

from dagster import IOManager, io_manager

def get_engine() -> sa.engine.Engine:
    """Create a sql alchemy engine from environment vars."""
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db = os.environ["POSTGRES_DB"]
    return sa.create_engine(f'postgresql://{user}:{password}@{db}:5432')
        
class DataframePostgresIOManager(IOManager):

    def handle_output(self, context, obj):
        # name is the name given to the Out that we're storing for
        table_name = context.metadata["table"]
        engine = get_engine()

        with engine.connect() as con:
            obj.to_sql(name=table_name, con=con, if_exists="replace",
                      index=False)

    def load_input(self, context):
        # upstream_output.name is the name given to the Out that we're loading for
        table_name = context.upstream_output.metadata["table"]
        engine = get_engine()
        with engine.connect() as con:
            return pd.read_sql_table(table_name, con)