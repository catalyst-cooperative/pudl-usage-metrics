"""Test usage metrics dagster jobs."""


### TEMP - Disable until Datasette intake is updated
# def test_datasette_job(datasette_partition_config, sqlite_engine):
#     """Process a single partition of datassette."""
#     usage_metrics_metadata.drop_all(sqlite_engine)
#     result = process_datasette_logs_locally.execute_in_process(
#         run_config=datasette_partition_config
#     )

#     assert result.success

#     # Make sure we got the correct number of rows.
#     with sqlite_engine.connect() as con:
#         logs = pd.read_sql(
#             "select insert_id from datasette_request_logs"
#             " where timestamp < '2022-02-06'",
#             con,
#         )
#     assert len(logs) == 891


# def test_primary_key_failure(datasette_partition_config, sqlite_engine):
#     """Reprocess the same partition as `test_datasette_job` test for integrity error."""
#     usage_metrics_metadata.drop_all(sqlite_engine)
#     result = process_datasette_logs_locally.execute_in_process(
#         run_config=datasette_partition_config
#     )

#     assert result.success

#     with pytest.raises(sa.exc.IntegrityError):
#         _ = process_datasette_logs_locally.execute_in_process(
#             run_config=datasette_partition_config
#         )
