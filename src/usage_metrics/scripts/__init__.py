"""The ``usage-metrics`` command-line interface.

`cli` is the top-level Click group (`usage-metrics`), wiring together the `etl`
command (drives the Dagster jobs), the `save` subgroup (snapshots metrics from
external APIs into GCS), and the `gaps` command (finds partitions with raw but no
processed data). It's installed as the `usage-metrics` console script.
"""

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}
"""Shared Click context settings: accept ``-h`` as well as ``--help`` everywhere."""
