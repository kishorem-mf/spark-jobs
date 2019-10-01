#!/usr/bin/env bash

echo "running contactpersons data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

DELTA_CONTACTS=delta_contacts.py
MATCH_CONTACTS=match_contacts.py

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar
SPARK_JOBS_EGG=${ARTEFACTS_DIR}name-matching/egg/string_matching.egg
PYTHON_DELTA_CONTACTS=${ARTEFACTS_DIR}name-matching/main/${DELTA_CONTACTS}
PYTHON_MATCH_CONTACTS=${ARTEFACTS_DIR}name-matching/main/${MATCH_CONTACTS}

RAW_CONTACTPERSONS_INPUT_PATH="${DATA_ROOT_DIR}raw/contactpersons/"

DATA_OPERATORS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/operators"
DATA_INVALID_EMAIL_INPUT="${DATA_ROOT_DIR}raw/reference/invalid_email.csv"
DATA_CONTACTPERSONS_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/contactpersons"
DATA_CONTACTPERSONS_RAW="${RAW_CONTACTPERSONS_INPUT_PATH}*.csv"
DATA_CONTACTPERSONS_INGESTED="${DATA_ROOT_DIR}ingested/common/contactpersons.parquet"
DATA_CONTACTPERSONS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/contactpersons"
DATA_CONTACTPERSONS_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/contactpersons_pre_processed.parquet"
DATA_CONTACTPERSONS_EXACT_MATCHES="${DATA_ROOT_DIR}intermediate/contactpersons_exact_matches.parquet"
DATA_CONTACTPERSONS_UNMATCHED_INTEGRATED="${DATA_ROOT_DIR}intermediate/contactpersons_unmatched_integrated.parquet"
DATA_CONTACTPERSONS_UNMATCHED_DELTA="${DATA_ROOT_DIR}intermediate/contactpersons_unmatched_delta.parquet"
DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA_INTEGRATED="${DATA_ROOT_DIR}intermediate/contactpersons_fuzzy_matched_delta_integrated.parquet"
DATA_CONTACTPERSONS_DELTA_LEFT_OVERS="${DATA_ROOT_DIR}intermediate/contactpersons_delta_left_overs.parquet"
DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA="${DATA_ROOT_DIR}intermediate/contactpersons_fuzzy_matched_delta.parquet"
DATA_CONTACTPERSONS_DELTA_GOLDEN_RECORDS="${DATA_ROOT_DIR}intermediate/contactpersons_delta_golden_records.parquet"
DATA_CONTACTPERSONS_COMBINED="${DATA_ROOT_DIR}intermediate/contactpersons_combined.parquet"
DATA_CONTACTPERSONS_UPDATED_REFERENCES="${DATA_ROOT_DIR}intermediate/contactpersons_updated_references.parquet"
DATA_CONTACTPERSONS_UPDATED_VALID_EMAIL="${DATA_ROOT_DIR}intermediate/contactpersons_updated_valid_email.parquet"
DATA_CONTACTPERSONS_CREATED_GOLDEN_RECORDS="${DATA_ROOT_DIR}output/integrated/contactpersons_golden"

echo
echo ContactPersonEmptyIntegratedWriter
spark-submit   --class="com.unilever.ohub.spark.ingest.initial.ContactPersonEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
               --outputFile=${DATA_CONTACTPERSONS_INTEGRATED_INPUT}

echo
echo ContactPersonConverter
spark-submit   --class="com.unilever.ohub.spark.ingest.common.ContactPersonConverter" ${SPARK_JOBS_JAR} \
               --inputFile=${DATA_CONTACTPERSONS_RAW} \
               --outputFile=${DATA_CONTACTPERSONS_INGESTED} \
               --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo ContactPersonPreProcess
spark-submit   --class="com.unilever.ohub.spark.merging.ContactPersonPreProcess" ${SPARK_JOBS_JAR} \
               --integratedInputFile=${DATA_CONTACTPERSONS_INTEGRATED_INPUT} \
               --deltaInputFile=${DATA_CONTACTPERSONS_INGESTED} \
               --deltaPreProcessedOutputFile=${DATA_CONTACTPERSONS_PRE_PROCESSED}

echo
echo ContactPersonIntegratedExactMatch
spark-submit   --class="com.unilever.ohub.spark.merging.ContactPersonIntegratedExactMatch" ${SPARK_JOBS_JAR} \
               --integratedInputFile=${DATA_CONTACTPERSONS_INTEGRATED_INPUT} \
               --deltaInputFile=${DATA_CONTACTPERSONS_PRE_PROCESSED} \
               --matchedExactOutputFile=${DATA_CONTACTPERSONS_EXACT_MATCHES} \
               --unmatchedIntegratedOutputFile=${DATA_CONTACTPERSONS_UNMATCHED_INTEGRATED} \
               --unmatchedDeltaOutputFile=${DATA_CONTACTPERSONS_UNMATCHED_DELTA}

spark-submit   --py-files=${SPARK_JOBS_EGG} ${PYTHON_DELTA_CONTACTS} \
               --integrated_input_path=${DATA_CONTACTPERSONS_UNMATCHED_INTEGRATED} \
               --ingested_daily_input_path=${DATA_CONTACTPERSONS_UNMATCHED_DELTA} \
               --updated_integrated_output_path=${DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA_INTEGRATED} \
               --unmatched_output_path=${DATA_CONTACTPERSONS_DELTA_LEFT_OVERS} \
               --country_code="TH"

spark-submit   --py-files=${SPARK_JOBS_EGG} ${PYTHON_MATCH_CONTACTS} \
               --input_file=${DATA_CONTACTPERSONS_DELTA_LEFT_OVERS} \
               --output_path=${DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA} \
               --country_code="TH"

echo
echo ContactPersonMatchingJoiner
spark-submit   --class="com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner" ${SPARK_JOBS_JAR} \
               --matchingInputFile=${DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA} \
               --entityInputFile=${DATA_CONTACTPERSONS_DELTA_LEFT_OVERS} \
               --outputFile=${DATA_CONTACTPERSONS_DELTA_GOLDEN_RECORDS}

echo
echo ContactPersonCombineExactAndFuzzyMatches
spark-submit   --class="com.unilever.ohub.spark.combining.ContactPersonCombineExactAndFuzzyMatches" ${SPARK_JOBS_JAR} \
               --exactMatchedInputFile=${DATA_CONTACTPERSONS_EXACT_MATCHES} \
               --fuzzyMatchedDeltaIntegratedInputFile=${DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA_INTEGRATED} \
               --deltaGoldenRecordsInputFile=${DATA_CONTACTPERSONS_DELTA_GOLDEN_RECORDS} \
               --combinedOutputFile=${DATA_CONTACTPERSONS_COMBINED}

echo
echo ContactPersonReferencing
spark-submit   --class="com.unilever.ohub.spark.merging.ContactPersonReferencing" ${SPARK_JOBS_JAR} \
               --combinedInputFile=${DATA_CONTACTPERSONS_COMBINED} \
               --operatorInputFile=${DATA_OPERATORS_INTEGRATED_OUTPUT} \
               --outputFile=${DATA_CONTACTPERSONS_UPDATED_REFERENCES}

spark-submit   --class="com.unilever.ohub.spark.merging.ContactPersonUpdateEmailValidFlag" ${SPARK_JOBS_JAR} \
               --contactPersonsInputFile=${DATA_CONTACTPERSONS_UPDATED_REFERENCES} \
               --invalidEmailAddressesInputFile=${DATA_INVALID_EMAIL_INPUT} \
               --outputFile=${DATA_CONTACTPERSONS_UPDATED_VALID_EMAIL}

echo
echo ContactPersonUpdateGoldenRecord
spark-submit   --class="com.unilever.ohub.spark.merging.ContactPersonUpdateGoldenRecord" ${SPARK_JOBS_JAR} \
               --inputFile=${DATA_CONTACTPERSONS_UPDATED_VALID_EMAIL} \
               --outputFile=${DATA_CONTACTPERSONS_INTEGRATED_OUTPUT}

echo
echo ContactPersonCreatePerfectGoldenRecord
spark-submit   --class="com.unilever.ohub.spark.merging.ContactPersonCreatePerfectGoldenRecord" ${SPARK_JOBS_JAR} \
               --inputFile=${DATA_CONTACTPERSONS_INTEGRATED_OUTPUT} \
               --outputFile=${DATA_CONTACTPERSONS_CREATED_GOLDEN_RECORDS}