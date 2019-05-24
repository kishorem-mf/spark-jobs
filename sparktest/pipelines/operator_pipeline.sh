#!/usr/bin/env bash

echo "running operator data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_OPERATORS_INPUT_PATH="${DATA_ROOT_DIR}raw/operators/"

DELTA_OPERATORS="delta_operators.py"
MATCH_OPERATORS="match_operators.py"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar
SPARK_JOBS_EGG=${ARTEFACTS_DIR}name-matching/egg/string_matching.egg
PYTHON_DELTA_OPERATORS=${ARTEFACTS_DIR}name-matching/main/$DELTA_OPERATORS
PYTHON_MATCH_OPERATORS=${ARTEFACTS_DIR}name-matching/main/$MATCH_OPERATORS

DATA_OPERATORS_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/operators"
DATA_OPERATORS_RAW="${RAW_OPERATORS_INPUT_PATH}*.csv"
DATA_OPERATORS_INGESTED="${DATA_ROOT_DIR}ingested/common/operators.parquet"
DATA_OPERATORS_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/operators_pre_processed.parquet"
DATA_OPERATORS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/operators"
DATA_OPERATORS_UPDATED_GOLDEN="${DATA_ROOT_DIR}intermediate/operators_updated_golden_records.parquet"
DATA_OPERATORS_UPDATED_INTEGRATED="${DATA_ROOT_DIR}intermediate/operators_fuzzy_matched_delta_integrated.parquet"
DATA_OPERATORS_DELTA_LEFT_OVERS="${DATA_ROOT_DIR}intermediate/operators_delta_left_overs.parquet"
DATA_OPERATORS_FUZZY_MATCHED_DELTA="${DATA_ROOT_DIR}intermediate/operators_fuzzy_matched_delta.parquet"
DATA_OPERATORS_DELTA_GOLDEN_RECORDS="${DATA_ROOT_DIR}intermediate/operators_delta_golden_records.parquet"
DATA_OPERATORS_COMBINED="${DATA_ROOT_DIR}intermediate/operators_combined.parquet"
DATA_OPERATORS_CREATED_GOLDEN_RECORDS="${DATA_ROOT_DIR}output/integrated/operators_golden"


DATA_CM_INTEGRATED_INPUT="${DATA_ROOT_DIR}output/integrated/channel_mappings"

spark-submit   --class="com.unilever.ohub.spark.ingest.initial.OperatorEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
               --outputFile=${DATA_OPERATORS_INTEGRATED_INPUT}

spark-submit   --class="com.unilever.ohub.spark.ingest.common.OperatorConverter" ${SPARK_JOBS_JAR} \
               --inputFile=${DATA_OPERATORS_RAW} \
               --outputFile=${DATA_OPERATORS_INGESTED} \
               --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

spark-submit   --class="com.unilever.ohub.spark.merging.OperatorPreProcess" ${SPARK_JOBS_JAR} \
               --integratedInputFile=${DATA_OPERATORS_INTEGRATED_INPUT} \
               --deltaInputFile=${DATA_OPERATORS_INGESTED} \
               --deltaPreProcessedOutputFile=${DATA_OPERATORS_PRE_PROCESSED}

spark-submit   --py-files=${SPARK_JOBS_EGG} ${PYTHON_DELTA_OPERATORS} \
               --integrated_input_path=${DATA_OPERATORS_INTEGRATED_INPUT} \
               --ingested_daily_input_path=${DATA_OPERATORS_PRE_PROCESSED} \
               --updated_integrated_output_path=${DATA_OPERATORS_UPDATED_INTEGRATED} \
               --unmatched_output_path=${DATA_OPERATORS_DELTA_LEFT_OVERS} \
               --min_norm_name_levenshtein_sim=0 \
               --country_code="DE"

spark-submit   --py-files=${SPARK_JOBS_EGG} ${PYTHON_MATCH_OPERATORS} \
               --input_file=${DATA_OPERATORS_DELTA_LEFT_OVERS} \
               --output_path=${DATA_OPERATORS_FUZZY_MATCHED_DELTA} \
               --min_norm_name_levenshtein_sim=0 \
               --country_code="DE"

spark-submit   --class="com.unilever.ohub.spark.merging.OperatorMatchingJoiner" ${SPARK_JOBS_JAR} \
               --matchingInputFile=${DATA_OPERATORS_FUZZY_MATCHED_DELTA} \
               --entityInputFile=${DATA_OPERATORS_DELTA_LEFT_OVERS} \
               --outputFile=${DATA_OPERATORS_DELTA_GOLDEN_RECORDS}

spark-submit   --class="com.unilever.ohub.spark.combining.OperatorCombining" ${SPARK_JOBS_JAR} \
               --integratedUpdated=${DATA_OPERATORS_UPDATED_INTEGRATED} \
               --newGolden=${DATA_OPERATORS_DELTA_GOLDEN_RECORDS} \
               --combinedEntities=${DATA_OPERATORS_COMBINED}

spark-submit   --class="com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord" ${SPARK_JOBS_JAR} \
               --inputFile=${DATA_OPERATORS_COMBINED} \
               --outputFile=${DATA_OPERATORS_UPDATED_GOLDEN}

spark-submit   --class="com.unilever.ohub.spark.merging.OperatorUpdateChannelMapping" ${SPARK_JOBS_JAR} \
               --operatorsInputFile=${DATA_OPERATORS_UPDATED_GOLDEN} \
               --channelMappingsInputFile=${DATA_CM_INTEGRATED_INPUT} \
               --outputFile=${DATA_OPERATORS_INTEGRATED_OUTPUT}

spark-submit   --class="com.unilever.ohub.spark.merging.OperatorCreateGoldenRecord" ${SPARK_JOBS_JAR} \
               --inputFile=${DATA_OPERATORS_INTEGRATED_OUTPUT} \
               --outputFile=${DATA_OPERATORS_CREATED_GOLDEN_RECORDS}            
