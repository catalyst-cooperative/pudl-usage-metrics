# Usage Metrics Dagster Pipeline

This project contains a dagster repository that extracts, cleans and loads PUDL's usage metrics into a data warehouse for analysis.

### Project Structure

This is the project structure generated by the [dagster cli](https://docs.dagster.io/getting-started/create-new-project#create-a-new-project):

| Name                   | Description                                                                       |
| ---------------------- | --------------------------------------------------------------------------------- |
| `README.md`            | A description and guide for this code repository                                  |
| `workspace.yaml`       | A file that specifies the location of the user code for Dagit and the Dagster CLI |
| `src/usage_metrics/`   | A Python directory that contains code for your Dagster repository                 |
| `tests/`               | A Python directory that contains tests for `usage_metrics`                        |
| `setup.py`             | A build script with Python package dependencies for this code repository          |

# Setup

## Conda Environment

We use the conda package manager to specify and update our development environment. We recommend using [miniconda](https://docs.conda.io/en/latest/miniconda.html) rather than the large pre-defined collection of scientific packages bundled together in the Anaconda Python distribution. You may also want to consider using [mamba](https://github.com/mamba-org/mamba) – a faster drop-in replacement for conda written in C++.

```
conda update conda
conda env create --name pudl-usage-metrics --file environment.yml
conda activate pudl-usage-metrics
```

## Environment Variables

The ETL uses [ipinfo](https://ipinfo.io/) to geocode ip addresses. You need to obtain an ipinfo API token and store it in the `IPINFO_TOKEN` environment variable.

If you want to take advantage of caching raw logs, rather than redownloading them for each run, you can set the optional ``DATA_DIR`` environment variable. If this is not set, the script will save files to a temporary directory by default. This is highly recommended to avoid unnecessary egress charges.

Dagster stores run logs and caches in a directory stored in the `DAGSTER_HOME` environment variable. The `usage_metrics/dagster_home/dagster.yaml` file contains configuration for the dagster instance. **Note:** The `usage_metrics/dagster_home/storage` directory could grow to become a couple GBs because all op outputs for every run are stored there. You can read more about the dagster_home directory in the [dagster docs](https://docs.dagster.io/deployment/dagster-instance#default-local-behavior).

To use the Kaggle API, [sign up for a Kaggle account](https://www.kaggle.com). Then go to the ['Account' tab]((https://www.kaggle.com/<username>/account)) of your user profile and select 'Create API Token'. This will trigger the download of `kaggle.json`, a file containing your API credentials. Use this file to automatically set your Kaggle API credentials locally, or manually set `KAGGLE_USER` and `KAGGLE_KEY` environment variables.

To set these environment variables, run these commands:

```
conda activate pudl-usage-metrics
conda env config vars set IPINFO_TOKEN="{your_api_key_here}"
conda env config vars set DAGSTER_HOME="$(pwd)/dagster_home/"
conda env config vars set DATA_DIR="$(pwd)/data/"
conda env config vars set KAGGLE_USER="{your_kaggle_username_here}" # If setting manually
conda env config vars set KAGGLE_KEY="{your_kaggle_api_key_here}" # If setting manually
conda activate pudl-usage-metrics
```

## Google Cloud Permissions

Ask the project admin of the `catalyst-cooperative-pudl` to add your email to the `pudl-usage-metrics-etl` group to acquire the adequate permissions to run the ETL locally. Once you have been added to the group, run:

```
gcloud auth application-default login
```

in your terminal. This command will prompt you to log in to your gmail account. Once completed, your Google credentials will be available in your environment.

## Git Pre-commit Hooks

Git hooks let you automatically run scripts at various points as you manage your source code. “Pre-commit” hook scripts are run when you try to make a new commit. These scripts can review your code and identify bugs, formatting errors, bad coding habits, and other issues before the code gets checked in. This gives you the opportunity to fix those issues before publishing them.

To make sure they are run before you commit any code, you need to enable the pre-commit hooks scripts with this command:

```
pre-commit install
```

The scripts that run are configured in the .pre-commit-config.yaml file.

## Deploy Dagster Locally

Now the environment is all set up and we can start up dagster!

## Set some global Dagster configs

In your ``DAGSTER_HOME`` folder, add a ``dagster.yaml`` file or edit your existing one to contain the following code:
```
run_queue:
  max_concurrent_runs: 1
```

When running backfills, this prevents you from kicking off 80 concurrent runs that will at worst crash your drive and best create SQL database errors.

### Dagster Daemon

In one terminal window start the dagster-daemon and UI by running these commands:

```
conda activate pudl-usage-metrics
dagster dev -m usage_metrics.etl
```

The [dagster-webserver](https://docs.dagster.io/concepts/webserver/ui) is a long-running service required for schedules, sensors and run queueing. The usage metrics ETL requires the daemon because the data is processed in partitions. Dagster kicks off individual runs for each [partition](https://docs.dagster.io/concepts/partitions-schedules-sensors/partitions) which are sent to a queue managed by the dagster-daemon.

This command simultaneously starts the dagit UI. This will launch dagit at [`http://localhost:3000/`](http://localhost:3000/). If you have another service running on port 3000 you can change the port by running:

```
dagster dev -m usage_metrics.etl -p {another_cool_port}
```

Dagster allows you to kick off [`backfills`](https://docs.dagster.io/concepts/partitions-schedules-sensors/backfills) and run partitions with specific configuration.

## Run the ETL

There is a job in the `usage_metrics/etl` sub package for each datasource (e.g datasette logs, github metrics…). Each job module contains a series of assets that extract, transform and load the data. When run locally, the IO manager will load data to a local sqlite database for development. When run on Github actions, the IO manager will load data to a Google Cloud SQL Postgres database for a Superset dashboard to access.

You can run the ETL via the dagit UI or the [dagster CLI](https://docs.dagster.io/_apidocs/cli).

### Backfills

To run a a complete backfill from the Dagit UI go to the job's partitions tab. Then click on the "Launch Backfill" button in the upper left corner of the window. This should bring up a new window with a list of partitions. Click "Select All" and then click the "Submit" button. This will submit a run for each partition. You can follow the runs on the ["Runs" tab](http://localhost:3000/instance/runs).

### Databases

#### SQLite

Jobs in the `local_usage_metrics` dagster repository create a sqlite database called `usage_metrics.db` in the `usage_metrics/data/` directory. A primary key constraint error will be thrown if you rerun the ETL for a partition. If you want to recreate the entire database just delete the sqlite database and rerun the ETL.

#### Google Cloud SQL Postgres

Jobs in the `gcp_usage_metrics` dagster repository append new partitions to tables in a Cloud SQL postgres database. A primary key constraint error will be thrown if you rerun the ETL for a partition. The `load-metrics` GitHub action is responsible for updating the database with new partitioned data.

If a new column is added or data is processed in a new way, you'll have to delete the table in the database and rerun a complete backfill. **Note: The Preset dashboard will be unavailable during the complete backfill.**

To run jobs in the `gcp_usage_metrics` repo, you need to whitelist your ip address for the database:

```
gcloud sql instances patch pudl-usage-metrics-db --authorized-networks={YOUR_IP_ADDRESS}
```

Then add the connection details as environment variables to your conda environment:

```
conda activate pudl-usage-metrics
conda env config vars set POSTGRES_IP={PUDL_USAGE_METRICS_DB_IP}
conda env config vars set POSTGRES_USER={PUDL_USAGE_METRICS_DB_USER}
conda env config vars set POSTGRES_PASSWORD={PUDL_USAGE_METRICS_DB_PASSWORD}
conda env config vars set POSTGRES_DB={PUDL_USAGE_METRICS_DB_DB}
conda env config vars set POSTGRES_PORT={PUDL_USAGE_METRICS_DB_PORT}
conda activate pudl-usage-metrics
```

You can find the connection details in the

### IP Geocoding with ipinfo

The ETL uses [ipinfo](https://ipinfo.io/) for geocoding the user ip addresses which provides 50k free API requests a month. The `usage_metrics.helpers.geocode_ip()` function using [joblib](https://joblib.readthedocs.io/en/latest/#main-features) to cache API calls so we don't call the API multiple times for a single ip address. The first time you run the ETL no API calls will be cached so the `geocode_ips()` op will take a while to complete.

## Add new data sources

To add a new data source to the dagster repo, add new modules to the `raw` and `core` and `out` directories and add these modules to the corresponding jobs. Once the dataset has been tested locally, run a complete backfill for the job that uses the `PostgresManager` to populate the Cloud SQL database.
