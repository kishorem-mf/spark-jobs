#!/usr/bin/env bash

echo "running subscription data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_SUBSCRIPTIONS_INPUT_PATH="${DATA_ROOT_DIR}raw/subscriptions/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_SUBSCRIPTIONS_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/subscriptions"
DATA_SUBSCRIPTIONS_INGESTED="${DATA_ROOT_DIR}ingested/common/subscriptions.parquet"
DATA_SUBSCRIPTIONS_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/subscriptions_pre_processed.parquet"
DATA_SUBSCRIPTIONS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/subscriptions"

DATA_CONTACTPERSONS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/contactpersons"

echo
echo SubscriptionEmptyIntegratedWriter
spark-submit    --class="com.unilever.ohub.spark.ingest.initial.SubscriptionEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_SUBSCRIPTIONS_INTEGRATED_INPUT}

echo
echo SubscriptionConverter
spark-submit    --class="com.unilever.ohub.spark.ingest.common.SubscriptionConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_SUBSCRIPTIONS_INPUT_PATH} \
                --outputFile=${DATA_SUBSCRIPTIONS_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo SubscriptionPreProcess
spark-submit    --class="com.unilever.ohub.spark.merging.SubscriptionPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_SUBSCRIPTIONS_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_SUBSCRIPTIONS_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_SUBSCRIPTIONS_PRE_PROCESSED}

echo
echo SubscriptionMerging
spark-submit    --class="com.unilever.ohub.spark.merging.SubscriptionMerging" ${SPARK_JOBS_JAR} \
                --subscriptionInputFile=${DATA_SUBSCRIPTIONS_PRE_PROCESSED} \
                --previousIntegrated=${DATA_SUBSCRIPTIONS_INTEGRATED_INPUT} \
                --contactPersonInputFile=${DATA_CONTACTPERSONS_INTEGRATED_OUTPUT} \
                --outputFile=${DATA_SUBSCRIPTIONS_INTEGRATED_OUTPUT}
