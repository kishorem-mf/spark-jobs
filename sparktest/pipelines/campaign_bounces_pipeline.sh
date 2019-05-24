#!/usr/bin/env bash

echo "running campaign_bounces data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/campaign_bounces/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/campaign_bounces"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/campaign_bounces.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/campaign_bounces_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/campaign_bounces"

DATA_OP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/operators"
DATA_CP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/contactpersons"

echo
echo CampaignBounceEmptyIntegratedWriter
spark-submit    --class="com.unilever.ohub.spark.ingest.initial.CampaignBounceEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

echo
echo CampaignBounceConverter
spark-submit    --class="com.unilever.ohub.spark.ingest.common.CampaignBounceConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo CampaignBouncePreProcess
spark-submit    --class="com.unilever.ohub.spark.merging.CampaignBouncePreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

echo
echo CampaignBounceMerging
spark-submit    --class="com.unilever.ohub.spark.merging.CampaignBounceMerging" ${SPARK_JOBS_JAR} \
                --campaignBounceInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --operatorIntegrated=${DATA_OP_INTEGRATED_INPUT} \
                --contactPersonIntegrated=${DATA_CP_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
