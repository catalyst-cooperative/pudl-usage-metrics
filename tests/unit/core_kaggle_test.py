"""Test the transform of raw Kaggle logs."""

import pandas as pd
from dagster import build_asset_context

from usage_metrics.core.kaggle import core_kaggle_logs
from usage_metrics.models import usage_metrics_schemas

# The shape of a daily Kaggle file as of 2026-09, flattened the way
# KaggleExtractor.load_file does with pd.json_normalize.
RAW_KAGGLE_RECORD = {
    "info": {
        "datasetId": 123,
        "datasetSlug": "pudl-project",
        "ownerUser": "catalystcooperative",
        "usabilityRating": 0.82,
        "totalViews": 6701,
        "totalVotes": 20,
        "totalDownloads": 2521,
        "title": "The Public Utility Data Liberation Project (PUDL)",
        "subtitle": "Energy data.",
        "description": "Electric utilities report a lot of information.",
        "keywords": ["united states", "energy"],
        "licenses": [{"name": "Attribution 4.0 International (CC BY 4.0)"}],
        "collaborators": [{"username": "someone", "role": "WRITER"}],
        "expectedUpdateFrequency": "not specified",
    },
    "metrics_date": "2026-09-08",
}


def _transform(record: dict) -> pd.DataFrame:
    with build_asset_context(partition_key="2026-09-08") as context:
        return core_kaggle_logs(context, pd.json_normalize(record))


def test_transformed_columns_are_all_in_the_schema() -> None:
    """Every column the transform produces must be in the schema.

    Columns missing from the schema would otherwise be silently dropped when the
    table is written.
    """
    transformed = _transform(RAW_KAGGLE_RECORD)

    schema_columns = set(usage_metrics_schemas["core_kaggle_logs"].columns)
    assert set(transformed.columns) <= schema_columns


def test_expected_update_frequency_is_kept() -> None:
    transformed = _transform(RAW_KAGGLE_RECORD)

    assert transformed["expected_update_frequency"].tolist() == ["not specified"]


def test_data_without_expected_update_frequency_still_transforms() -> None:
    """Older files don't have the field; the missing column is filled in on write."""
    record = {
        **RAW_KAGGLE_RECORD,
        "info": {
            k: v
            for k, v in RAW_KAGGLE_RECORD["info"].items()
            if k != "expectedUpdateFrequency"
        },
    }
    transformed = _transform(record)

    assert "expected_update_frequency" not in transformed.columns
    assert set(transformed.columns) <= set(
        usage_metrics_schemas["core_kaggle_logs"].columns
    )
