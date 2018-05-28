#!/usr/bin/env bash
AIRFLOW_HOME="/root/airflow"
AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////root/airflow/airflow.db"
AIRFLOW__CORE__LOAD_EXAMPLES="False"
AIRFLOW__CORE__EXECUTOR="SequentialExecutor"
AIRFLOW__CORE__DAGS_FOLDER="./dags"
AIRFLOW__CORE__BASE_LOG_FOLDER="./logs"

airflow initdb > /dev/null
airflow connections --add --conn_id "postgres_channels" --conn_uri "foo"
airflow variables --set "google_api_key" "foo"
airflow variables --set "slack_airflow_token" "foo"
airflow list_dags
s=`airflow list_dags`
exit `! [[ $s =~ Traceback ]]`
