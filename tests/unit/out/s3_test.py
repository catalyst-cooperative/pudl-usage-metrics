"""Tests for usage_metrics.out.s3."""

import pandas as pd
import pytest

from usage_metrics.out.s3 import (
    out_s3_daily_summary_by_db,
    out_s3_daily_summary_by_table,
    out_s3_daily_summary_by_user,
)


@pytest.fixture
def out_s3_logs() -> pd.DataFrame:
    """A small `out_s3_logs`-shaped frame, including rows with null IP fields."""
    return pd.DataFrame(
        {
            "time": pd.to_datetime(
                ["2026-09-25", "2026-09-25", "2026-09-25", "2026-09-25"]
            ),
            "table": [
                "core_eia860__scd_plants.parquet",
                "core_eia860__scd_plants.parquet",
                "core_eia923__monthly_generation.parquet",
                "core_eia923__monthly_generation.parquet",
            ],
            "version": ["v1", "v1", "v2", "v2"],
            "usage_type": [
                "other_s3",
                "other_s3",
                "eel_hole_link",
                "eel_hole_link",
            ],
            "megabytes_sent": [1.0, 2.0, 3.0, 4.0],
            "normalized_file_downloads": [0.5, 0.5, 1.0, 1.0],
            "request_uri": ["a", "b", "c", "d"],
            # Unresolved/masked IPs leave these null -- the id must still be
            # unique and non-null when that happens.
            "remote_ip": ["1.2.3.4", None, "5.6.7.8", None],
            "remote_ip_org": ["Google LLC", None, None, None],
            "remote_ip_country_name": ["United States", None, None, None],
        }
    )


@pytest.mark.parametrize(
    "summary_fn",
    [
        out_s3_daily_summary_by_table,
        out_s3_daily_summary_by_user,
        out_s3_daily_summary_by_db,
    ],
)
def test_daily_summary_id_is_unique_and_not_null(summary_fn, out_s3_logs) -> None:
    """Every summary row must get a non-null, unique id.

    Regression test: these assets never constructed an `id` column, so the
    Parquet IO manager backfilled it as all-null to match the declared schema,
    which fails the pandera uniqueness check on every row, every day.
    """
    result = summary_fn(out_s3_logs)
    assert not result["id"].isna().any()
    assert result["id"].is_unique
