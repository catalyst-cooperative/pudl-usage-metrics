"""Dagster Postgres IOManager."""

import os

import sqlalchemy as sa
from dagster import Field, io_manager

from usage_metrics.resources.sqldatabase import SQLIOManager


class PostgresIOManager(SQLIOManager):
    """Manage connection with a Postgres Database."""

    def __init__(
        self,
        user: str = os.environ.get("POSTGRES_USER", ""),
        password: str = os.environ.get("POSTGRES_PASSWORD", ""),
        db: str = os.environ.get("POSTGRES_DB", ""),
        ip: str = os.environ.get("POSTGRES_IP", ""),
        port: str = os.environ.get("POSTGRES_PORT", ""),
    ) -> None:
        """Initialize PostgresManager object."""
        self.engine = sa.create_engine(
            f"postgresql://{user}:{password}@{ip}:{port}/{db}"
        )
        self.datetime_column = "TIMESTAMP"


@io_manager(
    config_schema={
        "postgres_user": Field(
            str,
            description="Postgres connection string user.",
            default_value=os.environ.get("POSTGRES_USER", ""),
        ),
        "postgres_password": Field(
            str,
            description="Postgres connection string password.",
            default_value=os.environ.get("POSTGRES_PASSWORD", ""),
        ),
        "postgres_db": Field(
            str,
            description="Postgres connection string database.",
            default_value=os.environ.get("POSTGRES_DB", ""),
        ),
        "postgres_ip": Field(
            str,
            description="Postgres connection string ip address.",
            default_value=os.environ.get("POSTGRES_IP", ""),
        ),
        "postgres_port": Field(
            str,
            description="Postgres connection string port.",
            default_value=os.environ.get("POSTGRES_PORT", ""),
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
