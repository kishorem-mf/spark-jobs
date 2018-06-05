$PWD=(Get-Item -Path ".").FullName
$container="ulohub_airflow_dags"
echo "\n----------- stopping possibly running container --------------\n"
docker stop $container
echo "\n-----------       removing old container        --------------\n"
docker rm $container
$img='ulohubimages.azurecr.io/airflow:latest'
docker pull $img
docker run \
--env AIRFLOW_HOME="/root/airflow" \
--env AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:////root/airflow/airflow.db" \
--env AIRFLOW__CORE__SQL_ALCHEMY_POOL_SIZE="15" \
--env AIRFLOW__CORE__LOAD_EXAMPLES="False" \
--env AIRFLOW__CORE__EXECUTOR="SequentialExecutor" \
--env AIRFLOW__CORE__DAGS_FOLDER="/usr/app/dags" \
--env AIRFLOW__CORE__BASE_LOG_FOLDER="/usr/app/logs" \
--entrypoint "" \
-v $PWD:/usr/app \
-p 8070:8080 \
$img \
bash -c 'bash /usr/app/test.sh && airflow webserver'
