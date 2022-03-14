# set base image (host OS)
FROM python:3.9

RUN useradd -d /app/ metrics
USER metrics

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN python -m pip install -r /app/requirements.txt
RUN rm requirements.txt

# # Add the python packages to PATH
ENV PATH="/app/.local/bin:${PATH}"

# Add usage_metrics to PYTHONPATH
ENV PYTHONPATH="/app:${PYTHONPATH}"

# Setup dagster env
RUN mkdir -p /app/dagster_home
ENV DAGSTER_HOME=/app/dagster_home