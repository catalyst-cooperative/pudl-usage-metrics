"""Test that every schema in usage_metrics.models belongs to a persisted asset."""

from usage_metrics.etl import defs
from usage_metrics.models import usage_metrics_schemas


def test_every_schema_is_for_a_parquet_asset():
    """Schemas (and their pandera checks) are only for tables written to Parquet.

    An asset using the default IO manager is pickled, never written to Parquet, so
    its schema check would always fail with a FileNotFoundError.
    """
    io_manager_keys = {
        key.to_user_string(): asset_def.get_io_manager_key_for_asset_key(key)
        for asset_def in defs.resolve_asset_graph().assets_defs
        for key in asset_def.keys
    }
    not_persisted = {
        table_name: io_manager_keys.get(table_name, "<no such asset>")
        for table_name in usage_metrics_schemas
        if io_manager_keys.get(table_name) != "parquet_manager"
    }
    assert not not_persisted, (
        "These tables have schemas in usage_metrics.models but are not assets "
        f"written by parquet_manager: {not_persisted}"
    )
