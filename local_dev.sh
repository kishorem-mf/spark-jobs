#!/usr/bin/env bash

container_name="ulohub_airflow_dags"

printf "\n----------- stopping possibly running container --------------\n"
docker stop ${container_name}
printf "\n-----------       removing old container        --------------\n"
docker rm ${container_name}

printf "\n-----------      updating image if needed       --------------\n"
latest_tag="`az acr repository show-tags --name ulohubimages --repository airflow --output tsv | sort -gr | head -n 1`"
docker pull ulohubimages.azurecr.io/airflow:${latest_tag}

printf "\n-----------      starting airflow at 8070       --------------\n"
docker run \
--env AIRFLOW_HOME="/root/airflow" \
--env AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////root/airflow/airflow.db" \
--env AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE="15" \
--env AIRFLOW__CORE__LOAD_EXAMPLES="False" \
--env AIRFLOW__CORE__EXECUTOR="SequentialExecutor" \
--env AIRFLOW__CORE__DAGS_FOLDER="/root/airflow/dags" \
--env AIRFLOW__CORE__BASE_LOG_FOLDER="/root/airflow/logs" \
--mount type=bind,source="`pwd`/dags",target="/root/airflow/dags" \
-p 8070:8080 \
-d \
--name ${container_name} \
ulohubimages.azurecr.io/airflow:61 \
upgradedb_webserver

sleep 5

printf "\n--------- setting internal airflow configuration  ------------\n"
docker exec -i ${container_name} airflow pool --set "ohub_pool" "5" ""
docker exec -i ${container_name} airflow variables --set "google_api_key" "foo"
