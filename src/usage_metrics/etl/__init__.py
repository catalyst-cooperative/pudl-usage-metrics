"""Dagster definitions for the PUDL usage metrics ETL."""

import importlib.resources
import itertools
import warnings

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    Definitions,
    SourceAsset,
    asset_check,
    define_asset_job,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)
from dagster._core.definitions.cacheable_assets import CacheableAssetsDefinition

import usage_metrics
from usage_metrics.resources.postgres import PostgresManager
from usage_metrics.resources.sqlite import SQLiteManager

raw_module_groups = {
    "raw_s3": [usage_metrics.raw.s3],
    "raw_kaggle": [usage_metrics.raw.kaggle],
}

core_module_groups = {
    "core_s3": [usage_metrics.core.s3],
    "core_kaggle": [usage_metrics.core.kaggle],
}

out_module_groups = {}

all_asset_modules = raw_module_groups | core_module_groups | out_module_groups
default_assets = list(
    itertools.chain.from_iterable(
        load_assets_from_modules(
            modules,
            group_name=group_name,
        )
        for group_name, modules in all_asset_modules.items()
    )
)

default_asset_checks = list(
    itertools.chain.from_iterable(
        load_asset_checks_from_modules(
            modules,
        )
        for modules in all_asset_modules.values()
    )
)


# def asset_check_from_schema(
#     asset_key: AssetKey,
#     package: pudl.metadata.classes.Package,
# ) -> AssetChecksDefinition | None:
#     """Create a dagster asset check based on the resource schema, if defined."""
#     resource_id = asset_key.to_user_string()
#     try:
#         resource = package.get_resource(resource_id)
#     except ValueError:
#         return None
#     pandera_schema = resource.schema.to_pandera()

#     @asset_check(asset=asset_key, blocking=True)
#     def pandera_schema_check(asset_value) -> AssetCheckResult:
#         try:
#             pandera_schema.validate(asset_value, lazy=True)
#         except pr.errors.SchemaErrors as schema_errors:
#             return AssetCheckResult(
#                 passed=False,
#                 metadata={
#                     "errors": [
#                         {
#                             "failure_cases": str(err.failure_cases),
#                             "data": str(err.data),
#                         }
#                         for err in schema_errors.schema_errors
#                     ],
#                 },
#             )
#         return AssetCheckResult(passed=True)

#     return pandera_schema_check


def _get_keys_from_assets(
    asset_def: AssetsDefinition | SourceAsset | CacheableAssetsDefinition,
) -> list[AssetKey]:
    """Get a list of asset keys.

    Most assets have one key, which can be retrieved as a list from
    ``asset.keys``.

    Multi-assets have multiple keys, which can also be retrieved as a list from
    ``asset.keys``.

    SourceAssets always only have one key, and don't have ``asset.keys``. So we
    look for ``asset.key`` and wrap it in a list.

    We don't handle CacheableAssetsDefinitions yet.
    """
    if isinstance(asset_def, AssetsDefinition):
        return list(asset_def.keys)
    if isinstance(asset_def, SourceAsset):
        return [asset_def.key]
    return []


_asset_keys = itertools.chain.from_iterable(
    _get_keys_from_assets(asset_def) for asset_def in default_assets
)
# default_asset_checks += [
#     check
#     for check in (
#         asset_check_from_schema(asset_key, _package)
#         for asset_key in _asset_keys
#         if asset_key.to_user_string() != "core_epacems__hourly_emissions"
#     )
#     if check is not None
# ]

default_resources = {
    "sqlite_io_manager": SQLiteManager,
    "postgres_io_manager": PostgresManager,
}


defs: Definitions = Definitions(
    assets=default_assets,
    # asset_checks=default_asset_checks,
    resources=default_resources,
    jobs=[
        define_asset_job(
            name="all_logs_etl",
            description="This job ETLs logs for all metrics sources.",
        ),
        define_asset_job(
            name="s3_etl",
            description="This job ETLs S3 usage logs only.",
            selection=AssetSelection.groups("raw_s3", "core_s3"),
        ),
        define_asset_job(
            name="kaggle_etl",
            description="This job ETLs Kaggle usage logs only.",
            selection=AssetSelection.groups("raw_kaggle", "core_kaggle"),
        ),
    ],
)

"""A collection of dagster assets, resources, IO managers, and jobs for the PUDL ETL."""
