#!/usr/bin/env bash

echo "running campaign_opens data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/campaign_opens/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/campaign_opens"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/campaign_opens.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/campaign_opens_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/campaign_opens"

DATA_OP_GOLDEN_INTEGRATED_INPUT="${DATA_ROOT_DIR}output/integrated/operators_golden"
DATA_CP_GOLDEN_INTEGRATED_INPUT="${DATA_ROOT_DIR}output/integrated/contactpersons_golden"

echo
echo CampaignOpenEmptyIntegratedWriter
spark-submit    --class="com.unilever.ohub.spark.ingest.initial.CampaignOpenEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

echo
echo CampaignOpenConverter
spark-submit    --class="com.unilever.ohub.spark.ingest.common.CampaignOpenConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo CampaignOpenPreProcess
spark-submit    --class="com.unilever.ohub.spark.merging.CampaignOpenPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

echo
echo CampaignOpenMerging
spark-submit    --class="com.unilever.ohub.spark.merging.CampaignOpenMerging" ${SPARK_JOBS_JAR} \
                --campaignOpenInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --operatorGolden=${DATA_OP_GOLDEN_INTEGRATED_INPUT} \
                --contactPersonGolden=${DATA_CP_GOLDEN_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
