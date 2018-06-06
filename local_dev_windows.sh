#!/usr/bin/env bash
# This version is for windows
# Enter: <user> <password>


if [ "$#" -eq 0 ]
then
                echo Please try again by first entering password of ulohub_airflow_dags so that this script can work.
else
                container_name="ulohub_airflow_dags"

                full_source_path=$(pwd)
                # Windows path:
                source_path=${full_source_path:4:${#full_source_path}}/dags

                printf "\n----------- stopping possibly running container --------------\n\n"
                docker stop ${container_name}
                printf "\n-----------       removing old container        --------------\n\n"
                docker rm ${container_name}

                printf "\n-----------      updating image if needed       --------------\n\n"
                latest_tag="`az acr repository show-tags --name ulohubimages --repository airflow --output tsv | sort -gr | head -n 1`"
                docker login ulohubimages.azurecr.io -u ulohubimages -p $1
                docker pull ulohubimages.azurecr.io/airflow:${latest_tag}

                printf "\n-----------      starting airflow at 8070       --------------\n\n"
                docker run --env AIRFLOW_HOME="/root/airflow" --env AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////root/airflow/airflow.db" --env AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE="15" --env AIRFLOW__CORE__LOAD_EXAMPLES="False" --env AIRFLOW__CORE__EXECUTOR="SequentialExecutor" --env AIRFLOW__CORE__DAGS_FOLDER="/root/airflow/dags" --env AIRFLOW__CORE__BASE_LOG_FOLDER="/root/airflow/logs" -v ${source_path}:"/root/airflow/dags":ro -p 8070:8080 -d --name ${container_name} ulohubimages.azurecr.io/airflow:${latest_tag} upgradedb_webserver

                sleep 2

                printf "\n--------- setting internal airflow configuration  ------------\n\n"
                docker exec -d ${container_name} airflow pool --set "ohub_pool" "5" "foo"
                sleep 10
                docker exec -d ${container_name} airflow variables --set "google_api_key" "foo"
                sleep 10
                docker exec -d ${container_name} airflow connections --add --conn_id "postgres_channels" --conn_uri "foo"
                sleep 10
                docker exec -d ${container_name} airflow variables --set "slack_airflow_token" "foo"
                sleep 10

                echo 'list dags: docker exec -i ${container_name} airflow list_dags'
                docker exec -i ${container_name} airflow list_dags
fi
