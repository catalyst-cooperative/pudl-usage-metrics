"""Dagster definitions for the PUDL usage metrics ETL."""

import importlib.resources
import itertools
import os
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
from usage_metrics.resources.postgres import postgres_manager
from usage_metrics.resources.sqlite import sqlite_manager

raw_module_groups = {
    "raw_s3": [usage_metrics.raw.s3],
}

core_module_groups = {
    "core_s3": [usage_metrics.core.s3],
}

out_module_groups = {
    "out_s3": [usage_metrics.out.s3],
}

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

# resources_by_env = { # STILL TO DO!
#     "prod": {"io_manager": postgres_manager},
#     "local": {"io_manager": sqlite_manager},
# }

defs: Definitions = Definitions(
    assets=default_assets,
    # asset_checks=default_asset_checks,
    resources={"database_manager": sqlite_manager},  # TODO: How to handle this?
    jobs=[
        define_asset_job(
            name="all_metrics_etl",
            description="This job ETLs all metrics sources.",
        ),
        define_asset_job(
            name="s3_metrics_etl",
            description="This job ETLs logs for S3 usage logs only.",
            selection="*",
            tags={"source": "s3"},
        ),
    ],
)

"""A collection of dagster assets, resources, IO managers, and jobs for the PUDL ETL."""