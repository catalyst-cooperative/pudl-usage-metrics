"""Dagster definitions for the PUDL usage metrics ETL."""

import importlib.resources
import itertools
import logging
import os
import warnings

from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    Definitions,
    SourceAsset,
    define_asset_job,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)
from upath import UPath

import usage_metrics
from usage_metrics.checks import pandera_schema_checks
from usage_metrics.resources.parquet_io_manager import (
    PartitionedParquetIOManager,
    PyArrowTableReader,
)

logger = logging.getLogger(__name__)

raw_module_groups = {
    "raw_s3": [usage_metrics.raw.s3],
    "raw_github_partitioned": [usage_metrics.raw.github_partitioned],
    "raw_kaggle": [usage_metrics.raw.kaggle],
    "raw_zenodo": [usage_metrics.raw.zenodo],
    "raw_eel_hole": [usage_metrics.raw.eel_hole],
}

core_module_groups = {
    "core_s3": [usage_metrics.core.s3],
    "core_kaggle": [usage_metrics.core.kaggle],
    "core_github_partitioned": [usage_metrics.core.github_partitioned],
    "core_zenodo": [usage_metrics.core.zenodo],
    "core_eel_hole": [usage_metrics.core.eel_hole],
}

out_module_groups = {
    "out_s3": [usage_metrics.out.s3],
}

non_partitioned_module_groups = {
    "non_partitioned": [
        usage_metrics.raw.github_nonpartitioned,
        usage_metrics.core.github_nonpartitioned,
    ],
}

all_asset_modules = (
    raw_module_groups
    | core_module_groups
    | out_module_groups
    | non_partitioned_module_groups
)
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
    asset_def: AssetsDefinition | SourceAsset,
) -> list[AssetKey]:
    """Get a list of asset keys.

    Most assets have one key, which can be retrieved as a list from
    ``asset.keys``.

    Multi-assets have multiple keys, which can also be retrieved as a list from
    ``asset.keys``.

    SourceAssets always only have one key, and don't have ``asset.keys``. So we
    look for ``asset.key`` and wrap it in a list.
    """
    if isinstance(asset_def, AssetsDefinition):
        return list(asset_def.keys)
    if isinstance(asset_def, SourceAsset):
        return [asset_def.key]
    return []


_asset_keys = itertools.chain.from_iterable(
    _get_keys_from_assets(asset_def) for asset_def in default_assets
)

gcs_base_path = "gs://" + os.environ.get("GCS_BUCKET", "metrics.catalyst.coop")
local_base_path = str(UPath(os.environ.get("DATA_DIR", ".")) / "usage_metrics")

resources_by_env = {
    "prod": {
        "parquet_manager": PartitionedParquetIOManager(base_path=gcs_base_path),
        "pyarrow_reader": PyArrowTableReader(base_path=gcs_base_path),
    },
    "local": {
        "parquet_manager": PartitionedParquetIOManager(base_path=local_base_path),
        "pyarrow_reader": PyArrowTableReader(base_path=local_base_path),
    },
}

resources = resources_by_env[os.getenv("METRICS_PROD_ENV", "local")]

defs: Definitions = Definitions(
    assets=default_assets,
    asset_checks=default_asset_checks + pandera_schema_checks,
    resources=resources,
    jobs=[
        define_asset_job(
            name="all_partitioned_metrics_etl",
            description="This job ETLs all partitioned metrics sources.",
            selection=AssetSelection.all() - AssetSelection.groups("non_partitioned"),
        ),
        define_asset_job(
            name="all_nonpartitioned_metrics_etl",
            description="This job ETLs all non-partitioned metrics sources.",
            selection=AssetSelection.groups("non_partitioned")
            - AssetSelection.tag("disabled", "true"),
        ),
        define_asset_job(
            name="s3_metrics_etl",
            description="This job ETLs logs for S3 usage logs only.",
            selection=AssetSelection.tag("source", "s3"),
        ),
        define_asset_job(
            name="kaggle_metrics_etl",
            description="This job ETLs logs for Kaggle usage logs only.",
            selection=AssetSelection.tag("source", "kaggle"),
        ),
        define_asset_job(
            name="github_partitioned_metrics_etl",
            description="This job ETLs logs for Github partitioned usage logs only.",
            selection=AssetSelection.tag("source", "github_partitioned"),
        ),
        define_asset_job(
            name="github_nonpartitioned_metrics_etl",
            description="This job ETLs logs for Github non-partitioned usage logs only.",
            selection=AssetSelection.tag("source", "github_nonpartitioned")
            - AssetSelection.tag("disabled", "true"),
        ),
        define_asset_job(
            name="zenodo_metrics_etl",
            description="This job ETLs logs for Zenodo archives only.",
            selection=AssetSelection.tag("source", "zenodo"),
        ),
        define_asset_job(
            name="eel_hole_metrics_etl",
            description="This job ETLs logs for PUDL's data viewer only.",
            selection=AssetSelection.tag("source", "eel_hole"),
        ),
    ],
)

"""A collection of dagster assets, resources, IO managers, and jobs for the PUDL ETL."""
