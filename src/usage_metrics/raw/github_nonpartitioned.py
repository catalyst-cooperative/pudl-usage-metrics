"""Extract non-partitioned data from Github logs.

This includes stargazer and fork data, which when queried from Github returns data for
the entire history of the repository.
"""

from typing import Literal

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    asset,
)

from usage_metrics.raw.github_partitioned import (
    CUMULATIVE_METRIC_TYPES,
    GithubExtractor,
)


def cumulative_metrics_extraction_factory(
    metric: Literal[*CUMULATIVE_METRIC_TYPES],
) -> AssetsDefinition:
    """Create Dagster asset for each cumulatively-reported metric."""

    @asset(
        name=f"raw_github_{metric}",
        tags={"source": "github_nonpartitioned"},
    )
    def _raw_github_logs(context: AssetExecutionContext) -> pd.DataFrame:
        """Extract Github logs from the most recent files and return a DataFrame."""
        return GithubExtractor(metric=metric).extract(context)

    return _raw_github_logs


raw_github_nonpartitioned_assets = [
    cumulative_metrics_extraction_factory(metric) for metric in CUMULATIVE_METRIC_TYPES
]
