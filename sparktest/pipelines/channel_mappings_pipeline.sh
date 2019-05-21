#!/usr/bin/env bash

echo "running channelMappings data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/channel_mappings/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/channel_mappings"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/channel_mappings.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/channel_mappings_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/channel_mappings"

spark-submit    --class="com.unilever.ohub.spark.ingest.initial.ChannelMappingEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

spark-submit    --class="com.unilever.ohub.spark.ingest.common.ChannelMappingConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

spark-submit    --class="com.unilever.ohub.spark.merging.ChannelMappingPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

spark-submit    --class="com.unilever.ohub.spark.merging.ChannelMappingMerging" ${SPARK_JOBS_JAR} \
                --channelMappingsInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
