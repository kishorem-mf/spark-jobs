#!/usr/bin/env bash

host=${1}
username=${2}
password=${3}
dag_id=${4}
database="airflow"

query=$(cat <<-END
    delete from xcom where dag_id='${dag_id}';
    delete from task_instance where dag_id='${dag_id}';
    delete from sla_miss where dag_id='${dag_id}';
    delete from log where dag_id='${dag_id}';
    delete from job where dag_id='${dag_id}';
    delete from dag_run where dag_id='${dag_id}';
    delete from dag where dag_id='${dag_id}';
END
)

echo "Running query ${query}"

PGPASSWORD=${password} psql \
--host=${host} \
--port=5432 \
--username=${username} \
--d="dbname=${database} sslmode=require" \
<< THE_END
${query}
THE_END
