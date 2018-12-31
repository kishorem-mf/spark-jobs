#!/usr/bin/env bash

echo "running loyalty_points data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/loyalty_points/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/loyalty_points"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/loyalty_points.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/loyalty_points_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/loyalty_points"

DATA_OP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/operators"
DATA_CP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/contactpersons"

spark-submit    --class="com.unilever.ohub.spark.ingest.initial.LoyaltyPointsEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

spark-submit    --class="com.unilever.ohub.spark.ingest.common.LoyaltyPointsConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

spark-submit    --class="com.unilever.ohub.spark.merging.LoyaltyPointsPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

spark-submit    --class="com.unilever.ohub.spark.merging.LoyaltyPointsMerging" ${SPARK_JOBS_JAR} \
                --loyaltyPointsInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --operatorIntegrated=${DATA_OP_INTEGRATED_INPUT} \
                --contactPersonIntegrated=${DATA_CP_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
