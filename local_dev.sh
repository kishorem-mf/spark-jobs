#!/usr/bin/env bash
if [ "$#" -eq 0 ]; then
    echo 'Please enter password ACR password.'
    exit 1
fi
container="ulohub_airflow_dags"
printf "\n----------- stopping possibly running container --------------\n"
docker stop $container > /dev/null
printf "\n-----------       removing old container        --------------\n"
docker rm $container > /dev/null
img='ulohubimages.azurecr.io/airflow:latest'
docker pull $img
docker login ulohubimages.azurecr.io -u ulohubimages -p $1
docker run \
--env AIRFLOW_HOME="/root/airflow" \
--env AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////root/airflow/airflow.db" \
--env AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE="15" \
--env AIRFLOW__CORE__LOAD_EXAMPLES="False" \
--env AIRFLOW__CORE__EXECUTOR="SequentialExecutor" \
--env AIRFLOW__CORE__DAGS_FOLDER="/usr/app/dags" \
--env AIRFLOW__CORE__BASE_LOG_FOLDER="/usr/app/logs" \
-v $PWD:/usr/app \
-p 8070:8080 \
--entrypoint 'bash' \
$img \
-c 'echo 123'
# bash -c 'bash /usr/app/test.sh && airflow webserver'
