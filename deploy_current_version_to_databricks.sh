#!/usr/bin/env bash

# Make sure you have the databricks-cli installed and configured
# Make sure you have an env variable called $DATABRICKS_TOKEN

# This also assumes the cluster is already running :)

echo '------------------ building assembly jar --------------------'
sbt clean
sbt -DsparkDependencyType=provided assembly


echo '------------------ uploading to databricks --------------------'
fn="$(ls target/scala-2.11/*.jar | cut -d'/' -f3)"
targetFn="spark-jobs-assembly-WIP.jar"
path="dbfs:/libraries/ohub"

echo "uploading: ${fn} as ${targetFn} to ${path}"
databricks fs cp --overwrite target/scala-2.11/${fn} ${path}/${targetFn}

echo '------------------ removing old libary from cluster --------------------'
curl -i -d '{"cluster_id": "0425-080000-books393","libraries": [
{"jar": "dbfs:/libraries/ohub/spark-jobs-assembly-WIP.jar"},
{"egg": "dbfs:/libraries/ohub/spark-jobs-assembly-WIP.jar"},
{"jar": "dbfs:/FileStore/jars/f031751b_5b6a_4d59_87b3_1564170a647e-spark_jobs_assembly_0_2_0-9c8e5.jar"},
{"jar": "dbfs:/libraries/ohub/spark-jobs-assembly-0.2.0.jar"}]}' \
-H "Content-Type: application/json" \
-H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
-X POST https://westeurope.azuredatabricks.net/api/2.0/libraries/uninstall

sleep 10

echo '------------------ restarting cluster --------------------'
curl -i -d '{"cluster_id": "0425-080000-books393"}' \
-H "Content-Type: application/json" \
-H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
-X POST https://westeurope.azuredatabricks.net/api/2.0/clusters/restart
