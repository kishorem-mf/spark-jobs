#!/usr/bin/env bash
container="ulohub_airflow_dags"
latest_tag="`az acr repository show-tags --name ulohubimages --repository airflow --output tsv | sort -gr | head -n 1`" 
docker stop ${container}
docker rm ${container}
docker run \
--name ${container} \
--entrypoint "" \
--mount type=bind,source="`pwd`",target="/usr/local/airflow" \
--mount type=bind,source="`pwd`/dags",target="/usr/local/airflow/dags" \
--env AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////root/airflow/airflow.db" \
--env AIRFLOW_HOME="/root/airflow" \
--env AIRFLOW__CORE__LOAD_EXAMPLES="False" \
--env AIRFLOW__CORE__EXECUTOR="SequentialExecutor" \
--env AIRFLOW__CORE__DAGS_FOLDER="/usr/local/airflow/dags" \
--env AIRFLOW__CORE__BASE_LOG_FOLDER="/usr/local/airflow/logs" \
-p 8080:8080 \
ulohubimages.azurecr.io/airflow:${latest_tag} \
bash -c 'bash /usr/local/airflow/test.sh && airflow webserver'
