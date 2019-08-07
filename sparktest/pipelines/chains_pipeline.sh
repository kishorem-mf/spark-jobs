#!/usr/bin/env bash

echo "running chains data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/chains/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/chains"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/chains.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/chains_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/chains"

DATA_OP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/operators"
DATA_CP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/contactpersons"

echo
echo ChainEmptyIntegratedWriter
spark-submit    --class="com.unilever.ohub.spark.ingest.initial.ChainEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

echo
echo ChainConverter
spark-submit    --class="com.unilever.ohub.spark.ingest.common.ChainConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo ChainPreProcess
spark-submit    --class="com.unilever.ohub.spark.merging.ChainPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

echo
echo ChainMerging
spark-submit    --class="com.unilever.ohub.spark.merging.ChainMerging" ${SPARK_JOBS_JAR} \
                --chainsInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
