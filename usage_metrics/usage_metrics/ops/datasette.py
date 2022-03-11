import json
import os

import pandas as pd
from dagster import op, Out

import pandas_gbq
import pydata_google_auth
from google.oauth2 import service_account

JSON_FIELDS = ["resource", "http_request", "labels"]

@op(out=Out(metadata={"table": "raw_logs"}))
def extract():
    """Extract Datasette logs from BigQuery instance."""
    credentials = service_account.Credentials.from_service_account_file("/app/bigquery-service-account-key.json")

    project_id = 'catalyst-cooperative-pudl'

    raw_logs = pandas_gbq.read_gbq(
        "SELECT * FROM `datasette_logs.run_googleapis_com_requests`",
        project_id=project_id,
        credentials=credentials,
    )

    # Convert CamelCase to snake_case
    raw_logs.columns = raw_logs.columns.str.replace(r'(?<!^)(?=[A-Z])', '_').str.lower()

    # Convert the JSON fields into str because sqlalchemy can't write dicts to json
    for field in JSON_FIELDS:
        raw_logs[field] = raw_logs[field].apply(json.dumps)
    return raw_logs

@op(out=Out(metadata={"table": "unpacked_logs"}))
def unpack_httprequests(raw_logs):
    """Unpack http_request dict keys into separate fields."""
    # There are a couple of duplicates due to overlap between
    # properly saved logs starting 3/1/22 and the old logs from
    # 1/31/22 to 3/1/22
    # Convert the JSON fields into str because sqlalchemy can't write dicts to json
    for field in JSON_FIELDS:
        raw_logs[field] = raw_logs[field].apply(json.loads)

    logs = raw_logs.drop_duplicates(subset=["insert_id"])
    logs = logs.set_index("insert_id")
    assert logs.index.is_unique

    http_request_df = pd.DataFrame.from_dict(logs.http_request.to_dict(), orient='index')
    unpacked_logs = pd.concat([http_request_df, logs], axis=1)
    assert len(unpacked_logs) == len(logs)
    assert unpacked_logs.index.is_unique

    unpacked_logs.index.name = "insert_id"
    unpacked_logs = unpacked_logs.reset_index()

    unpacked_logs.columns = unpacked_logs.columns.str.replace(r'(?<!^)(?=[A-Z])', '_').str.lower()

    for field in JSON_FIELDS:
        unpacked_logs[field] = unpacked_logs[field].apply(json.dumps)

    return unpacked_logs