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


@pytest.mark.parametrize("unit", ["ms", "us", "ns"])
def test_daily_summary_id_does_not_depend_on_the_time_unit(unit, out_s3_logs) -> None:
    """The same row has the same id whatever unit its timestamp is stored in.

    Parquet files written before timestamps became microseconds hold milliseconds, so
    the ids already in production would otherwise change when a partition is rewritten.
    """
    out_s3_logs["time"] = out_s3_logs["time"].astype(f"datetime64[{unit}]")

    result = out_s3_daily_summary_by_table(out_s3_logs)

    assert set(result["id"]) == {
        "2026-09-25 00:00:00.000_eel_hole_link_core_eia923__monthly_generation.parquet_v2",
        "2026-09-25 00:00:00.000_other_s3_core_eia860__scd_plants.parquet_v1",
    }
