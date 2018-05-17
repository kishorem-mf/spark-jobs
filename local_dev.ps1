#Start in airflow-dags folder

$container_name = "ulohub_airflow_dags"
$dags_directory = (Get-Item -Path ".\dags").FullName

echo "\n----------- stopping possibly running container --------------\n"
docker stop $container_name

echo "\n-----------       removing old container        --------------\n"
docker rm $container_name

$latest_tag = az acr repository show-tags --name ulohubimages --repository airflow --output tsv | foreach-object { [Int] $_ } | sort -descending | select -first 1

echo "\n-----------      updating image if needed       --------------\n"
docker pull ulohubimages.azurecr.io/airflow:$latest_tag

echo "\n-----------      starting airflow at 8070       --------------\n"
docker run --env AIRFLOW_HOME="/root/airflow" --env AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////root/airflow/airflow.db" --env AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE="15" --env AIRFLOW__CORE__LOAD_EXAMPLES="False" --env AIRFLOW__CORE__EXECUTOR="SequentialExecutor" --env AIRFLOW__CORE__DAGS_FOLDER="/root/airflow/dags" --env AIRFLOW__CORE__BASE_LOG_FOLDER="/root/airflow/logs" --mount type=bind,source=$dags_directory,target="/root/airflow/dags" -p 8070:8080 -d --name ulohub_airflow_dags ulohubimages.azurecr.io/airflow:61 upgradedb_webserver

sleep 5

echo "\n--------- setting internal airflow configuration  ------------\n"

docker exec -i ulohub_airflow_dags airflow pool --set "ohub_pool" "5" "foo"
docker exec -i ulohub_airflow_dags airflow variables --set "google_api_key" "foo"
docker exec -i ulohub_airflow_dags airflow connections --add --conn_id "postgres_channels" --conn_uri "foo"
docker exec -i ulohub_airflow_dags airflow variables --set "slack_airflow_token" "foo"

sleep 1
docker exec -i ulohub_airflow_dags airflow list_dags
