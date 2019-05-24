#!/usr/bin/env bash

echo "running questions data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/questions/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/questions"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/questions.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/questions_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/questions"

DATA_OP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/operators"
DATA_CP_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/contactpersons"

echo
echo QuestionEmptyIntegratedWriter
spark-submit    --class="com.unilever.ohub.spark.ingest.initial.QuestionEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

echo
echo QuestionConverter
spark-submit    --class="com.unilever.ohub.spark.ingest.common.QuestionConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo QuestionPreProcess
spark-submit    --class="com.unilever.ohub.spark.merging.QuestionPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

echo
echo QuestionMerging
spark-submit    --class="com.unilever.ohub.spark.merging.QuestionMerging" ${SPARK_JOBS_JAR} \
                --questionsInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
