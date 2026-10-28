"""Dagster definitions for the PUDL usage metrics ETL."""

import itertools
import logging
import os

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)

import usage_metrics
from usage_metrics.checks import build_schema_checks
from usage_metrics.paths import get_gcs_base_path, get_parquet_dir
from usage_metrics.resources.parquet_io_manager import PartitionedParquetIOManager

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


# Only the selected environment's location is looked up, so a setting that matters
# only in production (e.g. PUDL_METRICS_GCS_BASE_PATH) can't break a local run.
if os.getenv("METRICS_PROD_ENV", "local") == "prod":
    parquet_base_path = get_gcs_base_path()
else:
    parquet_base_path = get_parquet_dir()

# The IO manager accepts a Path as well as a str, but its signature says only str.
resources = {
    "parquet_manager": PartitionedParquetIOManager(
        base_path=parquet_base_path  # type: ignore[bad-argument-type]
    )
}

defs: Definitions = Definitions(
    assets=default_assets,
    asset_checks=default_asset_checks + build_schema_checks(default_assets),
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
