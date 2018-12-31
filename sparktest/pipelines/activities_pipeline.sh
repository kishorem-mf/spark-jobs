#!/usr/bin/env bash

echo "running activities data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/activities/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/activities"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/activities.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/activities_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/activities"

DATA_OP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/operators"
DATA_CP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/contactpersons"

spark-submit    --class="com.unilever.ohub.spark.ingest.initial.ActivityEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

spark-submit    --class="com.unilever.ohub.spark.ingest.common.ActivityConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

spark-submit    --class="com.unilever.ohub.spark.merging.ActivityPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

spark-submit    --class="com.unilever.ohub.spark.merging.ActivityMerging" ${SPARK_JOBS_JAR} \
                --activitiesInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --operatorIntegrated=${DATA_OP_INTEGRATED_INPUT} \
                --contactPersonIntegrated=${DATA_CP_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
