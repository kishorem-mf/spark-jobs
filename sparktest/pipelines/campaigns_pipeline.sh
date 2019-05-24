#!/usr/bin/env bash

echo "running campaigns data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/campaigns/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/campaigns"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/campaigns.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/campaigns_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/campaigns"

DATA_CP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/contactpersons"

echo
echo CampaignEmptyIntegratedWriter
spark-submit    --class="com.unilever.ohub.spark.ingest.initial.CampaignEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

echo
echo CampaignConverter
spark-submit    --class="com.unilever.ohub.spark.ingest.common.CampaignConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo CampaignPreProcess
spark-submit    --class="com.unilever.ohub.spark.merging.CampaignPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

echo
echo CampaignMerging
spark-submit    --class="com.unilever.ohub.spark.merging.CampaignMerging" ${SPARK_JOBS_JAR} \
                --campaignInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --contactPersonIntegrated=${DATA_CP_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
