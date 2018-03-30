#!/usr/bin/env bash

# Make sure you have the databricks-cli installed and configured
# Make sure you have an env variable called $DATABRICKS_TOKEN

# This also assumes the cluster is already running :)

echo '------------------ building assembly jar --------------------'
sbt -DsparkDependencyType=provided assembly

echo '------------------ uploading to databricks --------------------'
databricks fs rm dbfs:/libraries/ohub/spark-jobs-assembly-WIP.jar
databricks fs cp target/scala-2.11/spark-jobs-assembly-0.1.jar dbfs:/libraries/ohub/spark-jobs-assembly-WIP.jar
databricks fs ls dbfs:/libraries/ohub

echo '------------------ removing old libary from cluster --------------------'
curl -i -d '{"cluster_id": "0314-131901-shalt605","libraries": [{"jar": "dbfs:/libraries/ohub/spark-jobs-assembly-WIP.jar"}, {"egg": "dbfs:/libraries/name_matching/string_matching.egg"}]}' \
-H "Content-Type: application/json" \
-H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
-X POST https://westeurope.azuredatabricks.net/api/2.0/libraries/uninstall

sleep 10

echo '------------------ restarting cluster --------------------'
curl -i -d '{"cluster_id": "0314-131901-shalt605"}' \
-H "Content-Type: application/json" \
-H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
-X POST https://westeurope.azuredatabricks.net/api/2.0/clusters/restart

