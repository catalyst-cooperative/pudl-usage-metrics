"""Entry point for ``dg``: load everything in the ``defs`` folder."""

from pathlib import Path

import dagster as dg


@dg.definitions
def defs() -> dg.Definitions:
    """Load all definitions found under ``usage_metrics/defs``."""
    return dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
