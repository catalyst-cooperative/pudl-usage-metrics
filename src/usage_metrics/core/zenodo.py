"""Transform data from Zenodo logs."""

import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    WeeklyPartitionsDefinition,
    asset,
)


@asset(
    partitions_def=WeeklyPartitionsDefinition(start_date="2023-08-16"),
    io_manager_key="database_manager",
    tags={"source": "zenodo"},
)
def core_zenodo_logs(
    context: AssetExecutionContext,
    raw_zenodo_logs: pd.DataFrame,
) -> pd.DataFrame:
    """Transform daily Zenodo logs."""
    context.log.info(f"Processing data for the week of {context.partition_key}")

    if raw_zenodo_logs.empty:
        context.log.warn(f"No data found for the week of {context.partition_key}")
        return raw_zenodo_logs

    # Drop columns
    df = raw_zenodo_logs.drop(
        columns=[
            "files",
            "owners",
            "revision",
            "metadata.access_right",
            "metadata.creators",
            "metadata.language",
            "metadata.resource_type.title",
            "metadata.resource_type.type",
            "metadata.license.id",
            "links.self_html",
            "links.doi",
            "links.self_doi",
            "links.self_doi_html",
            "links.parent",
            "links.parent_html",
            "links.parent_doi",
            "links.parent_doi_html",
            "links.self_iiif_manifest",
            "links.self_iiif_sequence",
            "links.files",
            "links.media_files",
            "links.archive",
            "links.archive_media",
            "links.latest",
            "links.latest_html",
            "links.versions",
            "links.draft",
            "links.reserve_doi",
            "links.access_links",
            "links.access_grants",
            "links.access_users",
            "links.access_request",
            "links.access",
            "links.communities",
            "links.communities-suggestions",
            "links.requests",
        ]
    )

    df = df.rename(
        columns={
            "created": "version_creation_date",  # Datetime
            "modified": "version_last_modified_date",  # Datetime
            "updated": "version_last_updated_date",  # Datetime
            "metadata.publication_date": "version_publication_date",  # Date
            "id": "version_id",
            "recid": "version_record_id",
            "conceptrecid": "concept_record_id",
            "doi": "version_doi",
            "conceptdoi": "concept_record_doi",
            "doi_url": "version_doi_url",
            "title": "version_title",
            "status": "version_status",
            "state": "version_state",
            "submitted": "version_submitted",
            "metadata.description": "version_description",
            "metadata.version": "version",
            "stats.downloads": "dataset_downloads",
            "stats.unique_downloads": "dataset_unique_downloads",
            "stats.views": "dataset_views",
            "stats.unique_views": "dataset_unique_views",
            "stats.version_downloads": "version_downloads",
            "stats.version_unique_downloads": "version_unique_downloads",
            "stats.version_views": "version_views",
            "stats.version_unique_views": "version_unique_views",
        }
    )

    # Convert string to date using Pandas
    for col in ["metrics_date", "version_publication_date"]:
        df[col] = pd.to_datetime(df[col])
        df[col] = df[col].dt.date

    # Convert string to datetime using Pandas
    for col in [
        "version_creation_date",
        "version_last_modified_date",
        "version_last_updated_date",
    ]:
        df[col] = pd.to_datetime(df[col])

    # Check validity of PK column
    df = df.set_index(["metrics_date", "version_id"])
    assert df.index.is_unique

    # Add a column with the dataset slug
    dataset_slugs = (
        {
            "Open Data for an Open Energy Transition": "ipi_presentation",
            "PUDL Raw EIA Annual Energy Outlook (AEO)": "eiaaeo",
            "PUDL Raw EIA Bulk Electricity API Data": "eia_bulk_elec",
            "PUDL Raw EIA Form 191 -- Monthly Underground Natural Gas Storage Report": "eia191",
            "PUDL Raw EIA Form 860": "eia860",
            "PUDL Raw EIA Form 860 -- Annual Electric Generator Report": "eia860",
            "PUDL Raw EIA Form 860M": "eia860m",
            "PUDL Raw EIA Form 861": "eia861",
            "PUDL Raw EIA Form 923": "eia923",
            "PUDL Raw EIA Form 923 -- Power Plant Operations Report": "eia923",
            "PUDL Raw EIA Form 930 -- Hourly and Daily Balancing Authority Operations Report": "eia930",
            "PUDL Raw EIA Thermoelectric Cooling Water": "eiawater",
            "PUDL Raw EPA CAMD to EIA Data Crosswalk": "epacamd_eia",
            "PUDL Raw EPA CEMS unitid to EIA Plant Crosswalk": "epacamd_eia",
            "PUDL Raw EPA Hourly Continuous Emission Monitoring System (CEMS)": "epacems",
            "PUDL Raw FERC Form 1": "ferc1",
            "PUDL Raw FERC Form 2": "ferc2",
            "PUDL Raw FERC Form 6": "ferc6",
            "PUDL Raw FERC Form 60": "ferc60",
            "PUDL Raw FERC Form 714": "ferc714",
            "PUDL Raw GridPath Resource Adequacy Toolkit Data": "gridpathatk",
            "PUDL Raw GridPath Resource Adequacy Toolkit Renewable Generation Profiles": "gridpathatk",
            "PUDL Raw Mine Safety and Health Administration (MSHA) Mines": "mshamines",
            "PUDL Raw NREL Annual Technology Baseline (ATB) for Electricity": "nrelatb",
            "PUDL Raw Pipelines and Hazardous Materials Safety Administration (PHMSA) Annual Natural Gas Report": "phmsagas",
            "Public Utility Data Liberation Project (PUDL) Data Release": "pudl",
            "The Public Utility Data Liberation (PUDL) Project": "pudl",
            "Workplace Democracy, Open Data, and Open Source": "csv_conf_presentation",
        }
        | {
            col: "pudl"
            for col in df.version_title.unique()
            if "catalyst-cooperative/pudl" in col or "PUDL Data Release" in col
        }
        | {
            col: "ferc_xbrl_extractor"
            for col in df.version_title.unique()
            if "catalyst-cooperative/ferc-xbrl-extractor" in col
        }
    )

    missed_mapping = df[
        ~df.version_title.isin(dataset_slugs.keys())
    ].version_title.unique()
    assert not missed_mapping, f"Missed mapping slugs for {missed_mapping}"

    # Assert we haven't missed any of the titles
    df["dataset_slug"] = df["version_title"].map(dataset_slugs)
    assert not df["dataset_slug"].isnull().to_numpy().any()

    context.log.info(f"Saving to {os.getenv("METRICS_PROD_ENV", "local")} environment.")

    return df.reset_index()
