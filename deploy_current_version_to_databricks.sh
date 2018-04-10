#!/usr/bin/env bash

# Make sure you have the databricks-cli installed and configured
# Make sure you have an env variable called $DATABRICKS_TOKEN

# This also assumes the cluster is already running :)

#echo '------------------ building assembly jar --------------------'
#sbt clean
#sbt -DsparkDependencyType=provided assembly
#
#
#echo '------------------ uploading to databricks --------------------'
#fn="$(ls target/scala-2.11/*.jar | cut -d'/' -f3)"
#targetFn="spark-jobs-assembly-WIP.jar"
#path="dbfs:/libraries/ohub"
#
#echo "uploading: ${fn} as ${targetFn} to ${path}"
#databricks fs rm ${path}/${targetFn}
#databricks fs cp target/scala-2.11/${fn} ${path}/${targetFn}

echo '------------------ removing old libary from cluster --------------------'
curl -i -d '{"cluster_id": "0314-131901-shalt605","libraries": [
{"jar": "dbfs:/libraries/ohub/spark-jobs-assembly-WIP.jar"},
{"jar": "dbfs:/libraries/ohub/spark-jobs-assembly-0.2.0.jar"},
{"jar": "dbfs:/libraries/spark-jobs-assembly-0.2.jar"},
{"egg": "dbfs:/libraries/string_matching.egg"}]}' \
-H "Content-Type: application/json" \
-H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
-X POST https://westeurope.azuredatabricks.net/api/2.0/libraries/uninstall

sleep 10

echo '------------------ restarting cluster --------------------'
curl -i -d '{"cluster_id": "0314-131901-shalt605"}' \
-H "Content-Type: application/json" \
-H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
-X POST https://westeurope.azuredatabricks.net/api/2.0/clusters/restart
