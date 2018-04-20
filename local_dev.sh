#!/usr/bin/env bash

#latest_tag="`az acr repository show-tags --name ulohubimages --repository airflow --output tsv | sort -gr | head -n 1`"
#docker pull ulohubimages.azurecr.io/airflow:${latest_tag}

docker run \
--env AIRFLOW_HOME="/root/airflow" \
--env AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////root/airflow/airflow.db" \
--env AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE="15" \
--env AIRFLOW__CORE__LOAD_EXAMPLES="False" \
--env AIRFLOW__CORE__EXECUTOR="SequentialExecutor" \
--env AIRFLOW__CORE__DAGS_FOLDER="/root/airflow/dags" \
--env AIRFLOW__CORE__BASE_LOG_FOLDER="/root/airflow/logs" \
--mount type=bind,source="`pwd`/dags",target="/root/airflow/dags" \
-p 8080:8080 \
ulohubimages.azurecr.io/airflow:61 \
upgradedb_webserver
