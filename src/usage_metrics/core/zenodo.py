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

    # Rename columns
    df = raw_zenodo_logs.rename(
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
            "swh.swhid": "software_hash_id_legacy",
            "swh": "software_hash_id",  # Updated in mid October 2025
        }
    )

    # De-duplicate software_hash_id columns
    # Older data will only have the legacy column, newer data should only have the
    # software_hash_id field, and data during the transition may have both.
    if "software_hash_id" in df.columns:
        if "software_hash_id_legacy" in df.columns:
            # Handle overlapping data
            df["software_hash_id"] = df["software_hash_id"].fillna(
                df["software_hash_id_legacy"]
            )
    else:
        if "software_hash_id_legacy" in df.columns:
            # Handle older data
            df["software_hash_id"] = df["software_hash_id_legacy"]

    # Drop columns
    df = df.drop(
        columns=["files", "owners", "revision", "software_hash_id_legacy"]
    ).drop(
        columns=[col for col in df.columns if col.startswith(("metadata.", "links."))]
    )
    # Column names vary by Zenodo archive type, so we drop any remaining metadata and link columns

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
    dataset_slugs = {
        "10723220": "ipi_presentation",
        "10838487": "eiaaeo",
        "7067366": "eiaapi",
        "7682357": "eia176",
        "10607836": "eia191",
        "10607838": "eia757a",
        "4127026": "eia860",
        "4281336": "eia860m",
        "4127028": "eia861",
        "4127039": "eia923",
        "10840077": "eia930",
        "7683135": "eiawater",
        "6633769": "epacamd_eia",
        "10233185": "epacems",
        "4127043": "ferc1",
        "5879542": "ferc2",
        "7126395": "ferc6",
        "7126434": "ferc60",
        "4127100": "ferc714",
        "10844661": "gridpathatk",
        "7683517": "mshamines",
        "10839267": "nrelatb",
        "7683351": "phmsagas",
        "11402753": "csv_conf_2024_coops",
        "13937522": "vcerare",
        "13948331": "naps2024",
        "11455506": "csv_conf_2024_pudl",
        "13919959": "vcerare",
        "3653158": "pudl_data_release",
        "3404014": "pudl_code",
        "10020145": "ferc_xbrl_extractor",
        "14624611": "censuspep",
        "14757121": "doeiraec",
        "14757598": "epapcap",
        "14758684": "doelead",
        "14767206": "eianems",
        "14767235": "epaegrid",
        "14782473": "eiacbecs",
        "14736285": "usgsuspvdb",
        "14782873": "nrelefs",
        "14783182": "nrelsts",
        "14783184": "nrelss",
        "14783214": "usgswtdb",
        "14783267": "eiarecs",
        "14783043": "epamats",
        "14888356": "nrelsiting",
        "15312754": "eiasteo",
        "10086108": "ferceqr",
        "2825634": "csv_conf_2019_pudl",
        "3677547": "ferc1_sqlite",
        "4127048": "censusdp1tract",
        "4739559": "csv_conf_2021_pudl",
        "5348395": "hourly_demand_by_state",
        "4127054": "epacems_old",
    }

    missed_mapping = df[
        ~df.concept_record_id.isin(dataset_slugs.keys())
    ].concept_record_id
    assert missed_mapping.empty, f"Missed mapping slugs for {missed_mapping.unique()}"

    # Assert we haven't missed any of the titles
    df["dataset_slug"] = df["concept_record_id"].map(dataset_slugs)
    assert not df["dataset_slug"].isnull().to_numpy().any()

    context.log.info(f"Saving to {os.getenv('METRICS_PROD_ENV', 'local')} environment.")

    return df.reset_index()
