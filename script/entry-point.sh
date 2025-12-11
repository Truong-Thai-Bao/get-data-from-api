#!/bin/bash
set -e

# Install requirements if exists
#if [ -e "/opt/airflow/requirements.txt" ]; then
#    $(command -v pip) install --user -r requirements.txt
#fi

# Initialize Airflow DB and create user if DB not exists
if [ ! -f "/opt/airflow/airflow.db" ]; then
    airflow db init && \
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin
fi

# Upgrade DB
$(command -v airflow) db upgrade

# Start webserver
exec "$@"
