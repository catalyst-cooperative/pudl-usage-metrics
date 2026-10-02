"""Tests for usage_metrics.core.zenodo."""

import json
from unittest.mock import patch

import pandas as pd
import pytest
from dagster import build_asset_context

from usage_metrics.core.zenodo import core_zenodo_logs
from usage_metrics.raw.zenodo import ZenodoExtractor

CONCEPT = "7067366"  # eiaapi, one of the datasets with a slug


def _record(version_id: int, downloads: int) -> dict:
    """One version of a record, with the fields core_zenodo_logs needs."""
    return {
        "id": version_id,
        "recid": version_id,
        "conceptrecid": CONCEPT,
        "doi": f"10.5281/zenodo.{version_id}",
        "conceptdoi": f"10.5281/zenodo.{CONCEPT}",
        "doi_url": f"https://doi.org/10.5281/zenodo.{version_id}",
        "title": "A dataset",
        "status": "published",
        "state": "done",
        "submitted": True,
        "created": "2026-01-01T00:00:00+00:00",
        "modified": "2026-01-02T00:00:00+00:00",
        "updated": "2026-01-03T00:00:00+00:00",
        "metadata": {
            "publication_date": "2026-01-01",
            "description": "A description.",
            "version": "v1",
        },
        "stats": {
            "downloads": downloads,
            "unique_downloads": downloads,
            "views": downloads,
            "unique_views": downloads,
            "version_downloads": downloads,
            "version_unique_downloads": downloads,
            "version_views": downloads,
            "version_unique_views": downloads,
        },
    }


def _raw(tmp_path, archives: dict[str, list[dict]]) -> pd.DataFrame:
    """What raw_zenodo_logs gives: the archives, as files named <date>-<id>.json."""
    frames = []
    for name, records in archives.items():
        path = tmp_path / name
        path.write_text(json.dumps({"hits": {"hits": records}}))
        frames.append(ZenodoExtractor().load_file(path))
    return pd.concat(frames, ignore_index=True)


def _core(raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Run core_zenodo_logs, returning its result and the warnings it logged."""
    with (
        build_asset_context(partition_key="2026-10-01") as context,
        patch.object(context.log, "warning") as warning,
    ):
        result = core_zenodo_logs(context, raw)
    assert isinstance(result, pd.DataFrame)
    return result, [call.args[0] for call in warning.call_args_list]


def test_a_version_in_two_archives_of_one_day_keeps_the_latest(tmp_path):
    """The archive job ran twice on 2026-10-01 and a version was published in between.

    The second archive is named for the new latest version, and has all the versions of
    the first, with newer statistics. Without dropping the older copy, the table's
    primary key is not unique.
    """
    raw = _raw(
        tmp_path,
        {
            "2026-10-01-100.json": [_record(1, downloads=10), _record(100, 10)],
            "2026-10-01-200.json": [
                _record(1, downloads=15),
                _record(100, 15),
                _record(200, 1),
            ],
        },
    )

    result, warnings = _core(raw)

    assert sorted(result["version_id"]) == [1, 100, 200]
    downloads = dict(
        zip(result["version_id"], result["version_downloads"], strict=True)
    )
    assert downloads == {1: 15, 100: 15, 200: 1}  # the later archive's numbers
    assert "source_record_id" not in result.columns
    assert len(warnings) == 1
    assert "2 versions are in more than one archive" in warnings[0]
    assert "2026-10-01" in warnings[0]


def test_the_same_version_on_different_days_is_kept_for_each_day(tmp_path):
    raw = _raw(
        tmp_path,
        {
            "2026-10-01-100.json": [_record(100, downloads=10)],
            "2026-10-02-100.json": [_record(100, downloads=12)],
        },
    )

    result, warnings = _core(raw)

    assert len(result) == 2
    assert warnings == []


def test_unique_versions_are_not_changed(tmp_path):
    raw = _raw(tmp_path, {"2026-10-01-100.json": [_record(1, 10), _record(100, 10)]})

    result, warnings = _core(raw)

    assert sorted(result["version_id"]) == [1, 100]
    assert warnings == []


def test_a_file_name_without_a_record_id_is_rejected(tmp_path):
    path = tmp_path / "2026-10-01.json"
    path.write_text(json.dumps({"hits": {"hits": [_record(1, 10)]}}))

    with pytest.raises(ValueError, match="record id"):
        ZenodoExtractor().load_file(path)
