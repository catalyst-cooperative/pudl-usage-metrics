"""Test how the pandera schema asset checks are wired into the Dagster jobs."""

from usage_metrics.etl import defs


def _upstream_nodes(job_name: str, node_name: str) -> set[str]:
    job = defs.resolve_job_def(job_name)
    upstream_outputs = (
        job.graph.dependency_structure.input_to_upstream_outputs_for_node(node_name)
    )
    return {output.node_name for outs in upstream_outputs.values() for output in outs}


def test_schema_check_blocks_assets_downstream_of_the_table() -> None:
    """A table whose data fails its schema must not feed anything downstream.

    For example, if AWS adds a field to the S3 logs and the columns are misaligned,
    the format checks fail, and out_s3_logs and the summaries built on it mustn't run.
    """
    assert "core_s3_logs_pandera_schema_check" in _upstream_nodes(
        "s3_metrics_etl", "out_s3_logs"
    )
