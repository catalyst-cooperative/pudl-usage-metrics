"""Dagster ops for intake logs."""

from io import BytesIO

import google.auth
import pandas as pd
from dagster import AssetMaterialization, Out, Output, graph, op
from google.cloud import storage
from tqdm import tqdm

from usage_metrics.helpers import str_to_datetime
from usage_metrics.ops.datasette import geocode_ips


def create_rename_mapping(raw_logs: pd.DataFrame) -> dict:
    """Create rename column mappings from raw intake logs.

    Args:
        raw_logs: Dataframe of raw Intake logs.

    Returns:
        columns_mappings: Mapping of old to new column names.
    """
    column_mappings = {
        "c_ip": "remote_ip",
        "c_ip_type": "remote_ip_type",
        "c_ip_region": "remote_ip_region",
        "time_taken_micros": "response_time_taken",
        "time_micros": "timestamp",
        "s_request_id": "insert_id",
    }

    for field in raw_logs.columns:
        if field.startswith("cs"):
            column_mappings[field] = "request" + field.removeprefix("cs")
        elif field.startswith("sc"):
            column_mappings[field] = "response" + field.removeprefix("sc")

    return column_mappings


@op(out={"raw_logs": Out(is_required=False)})
def extract(context) -> pd.DataFrame:
    """Extract intake logs from Google Cloud Storage.

    Returns:
        raw_logs: Dataframe of intake logs.
    """
    credentials, project_id = google.auth.default()
    bucket_url = "intake-logs"
    bucket = storage.Client(credentials=credentials).bucket(
        bucket_url, user_project=project_id
    )
    assert bucket.exists(), f"{bucket_url} does not exist."

    logs = []

    start_date = str_to_datetime(context.op_config["start_date"])
    end_date = str_to_datetime(context.op_config["end_date"])

    # Intake storage bucket usage logs are saved every hour.
    # GCP also saves storage logs every day.
    # We are only interested in processing the usage logs.
    for blob in tqdm(bucket.list_blobs()):
        if "usage" in blob.name:
            # Get the batch of logs for the given partition.
            if blob.time_created >= start_date and blob.time_created < end_date:
                logs.append(pd.read_csv(BytesIO(blob.download_as_bytes())))

    # Skip downstream steps if there are no logs to process.
    if not logs:
        return

    raw_logs = pd.concat(logs)

    # rename the columns
    column_mappings = create_rename_mapping(raw_logs)
    raw_logs = raw_logs.rename(columns=column_mappings)

    # set index
    assert raw_logs.insert_id.is_unique
    raw_logs = raw_logs.set_index("insert_id")

    # Skip downstream steps if there are no logs to process.
    if len(raw_logs) > 0:
        yield Output(raw_logs, output_name="raw_logs")


@op(out={"intake_logs": Out(is_required=False)})
def filter_intake_logs(context, raw_logs):
    """Filter get logs from python client."""
    # Get all request logs produced by python client.
    intake_logs = raw_logs[
        raw_logs.request_user_agent.str.lower().str.contains("python")
    ]

    # Get all get requests
    intake_logs = intake_logs.query("request_operation == 'storage.objects.get'")

    # Convert unix epoch to datetime
    intake_logs["timestamp"] = pd.to_datetime(
        intake_logs.timestamp, origin="unix", unit="us"
    )

    # Remove unused fields
    intake_logs = intake_logs.drop(columns=["remote_ip_region"])

    # Skip downstream steps if there are no logs to process.
    if len(intake_logs) > 0:
        yield Output(intake_logs, output_name="intake_logs")


@op
def clean_object_name(context, intake_logs: pd.DataFrame) -> pd.DataFrame:
    """Remove prefix and extract tag and object name from request_object."""
    # Get everything after the first backslash
    intake_logs["object_path"] = (
        intake_logs["request_object"]
        .str.extract(r"(\/([\s\S]*)$)", expand=False)[1]
        .replace("", pd.NA)
    )
    # Get everything before the first backslash
    intake_logs["tag"] = intake_logs["request_object"].str.extract(
        r"^([^/]+?)(\s*[/])"
    )[0]

    intake_logs = intake_logs.reset_index()
    return intake_logs


@graph
def transform(raw_logs: pd.DataFrame) -> pd.DataFrame:
    """Transform intake logs."""
    intake_logs = filter_intake_logs(raw_logs)
    intake_logs = clean_object_name(intake_logs)
    intake_logs = geocode_ips(intake_logs)
    return intake_logs


@op(required_resource_keys={"database_manager"})
def load(context, clean_intake_logs: pd.DataFrame) -> None:
    """Load clean intake logs to a database."""
    context.resources.database_manager.append_df_to_table(
        clean_intake_logs, "intake_logs"
    )
    context.log_event(
        AssetMaterialization(
            asset_key="intake_logs",
            description="Clean intake logs from intake.",
            partition=context.get_mapping_key(),
            metadata={
                "Number of Rows:": len(clean_intake_logs),
                "Min Date": str(clean_intake_logs.timestamp.min()),
                "Max Date": str(clean_intake_logs.timestamp.max()),
            },
        )
    )
