ARG BASEIMAGE=scalaspark

FROM $BASEIMAGE

# copy artefacts
ARG ARTEFACTS_DIR=/usr/local/artefacts/
RUN mkdir $ARTEFACTS_DIR
COPY artefacts/* $ARTEFACTS_DIR

# copy test input data TODO USE PROPER TEST DATA
ARG DATA_ROOT_DIR=/usr/local/data/
# operators
ARG RAW_OPERATORS_INPUT_PATH=${DATA_ROOT_DIR}raw/operators/common/
RUN mkdir -p $RAW_OPERATORS_INPUT_PATH
COPY data/operators/* $RAW_OPERATORS_INPUT_PATH
# contactpersons
ARG RAW_CONTACTPERSONS_INPUT_PATH=${DATA_ROOT_DIR}raw/contactpersons/common/
RUN mkdir -p $RAW_CONTACTPERSONS_INPUT_PATH
COPY data/contactpersons/* $RAW_CONTACTPERSONS_INPUT_PATH

# start spark cluster with 2 worker nodes
RUN start-master.sh
RUN start-slave.sh spark://spark:7077
RUN start-slave.sh spark://spark:7077

ARG SPARK_SUBMIT=spark-submit
ARG SPARK_JOBS_JAR=${ARTEFACTS_DIR}spark-jobs-assembly-0.2.0.jar
ARG SPARK_JOBS_EGG=${ARTEFACTS_DIR}string_matching.egg
ARG PYTHON_DELTA_OPERATORS=${ARTEFACTS_DIR}delta_operators.py
ARG PYTHON_MATCH_OPERATORS=${ARTEFACTS_DIR}match_operators.py
ARG PYTHON_DELTA_CONTACTS=${ARTEFACTS_DIR}delta_contacts.py
ARG PYTHON_MATCH_CONTACTS=${ARTEFACTS_DIR}match_contacts.py

# START OPERATORS INTEGRATION TEST

ARG DATA_OPERATORS_INTEGRATED_INPUT=${DATA_ROOT_DIR}input/integrated/operators
ARG DATA_OPERATORS_RAW=$RAW_OPERATORS_INPUT_PATH*.csv
ARG DATA_OPERATORS_INGESTED=${DATA_ROOT_DIR}ingested/common/operators.parquet
ARG DATA_OPERATORS_INTEGRATED_OUTPUT=${DATA_ROOT_DIR}output/integrated/operators
ARG DATA_OPERATORS_UPDATED_INTEGRATED=${DATA_ROOT_DIR}intermediate/operators_fuzzy_matched_delta_integrated.parquet
ARG DATA_OPERATORS_DELTA_LEFT_OVERS=${DATA_ROOT_DIR}intermediate/operators_delta_left_overs.parquet
ARG DATA_OPERATORS_FUZZY_MATCHED_DELTA=${DATA_ROOT_DIR}intermediate/operators_fuzzy_matched_delta.parquet
ARG DATA_OPERATORS_DELTA_GOLDEN_RECORDS=${DATA_ROOT_DIR}intermediate/operators_delta_golden_records.parquet
ARG DATA_OPERATORS_COMBINED=${DATA_ROOT_DIR}intermediate/operators_combined.parquet

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.ingest.initial.OperatorEmptyIntegratedWriter" $SPARK_JOBS_JAR \
                    --outputFile=$DATA_OPERATORS_INTEGRATED_INPUT

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.ingest.common.OperatorConverter" $SPARK_JOBS_JAR \
                    --inputFile=$DATA_OPERATORS_RAW \
                    --outputFile=$DATA_OPERATORS_INGESTED \
                    --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

RUN $SPARK_SUBMIT   --py-files=$SPARK_JOBS_EGG $PYTHON_DELTA_OPERATORS \
                    --integrated_input_path=$DATA_OPERATORS_INTEGRATED_INPUT \
                    --ingested_daily_input_path=$DATA_OPERATORS_INGESTED \
                    --updated_integrated_output_path=$DATA_OPERATORS_UPDATED_INTEGRATED \
                    --unmatched_output_path=$DATA_OPERATORS_DELTA_LEFT_OVERS \
                    --country_code="TR"

RUN $SPARK_SUBMIT   --py-files=$SPARK_JOBS_EGG $PYTHON_MATCH_OPERATORS \
                    --input_file=$DATA_OPERATORS_DELTA_LEFT_OVERS \
                    --output_path=$DATA_OPERATORS_FUZZY_MATCHED_DELTA \
                    --country_code="TR"

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.merging.OperatorMatchingJoiner" $SPARK_JOBS_JAR \
                    --matchingInputFile=$DATA_OPERATORS_FUZZY_MATCHED_DELTA \
                    --entityInputFile=$DATA_OPERATORS_DELTA_LEFT_OVERS \
                    --outputFile=$DATA_OPERATORS_DELTA_GOLDEN_RECORDS

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.combining.OperatorCombining" $SPARK_JOBS_JAR \
                    --integratedUpdated=$DATA_OPERATORS_UPDATED_INTEGRATED \
                    --newGolden=$DATA_OPERATORS_DELTA_GOLDEN_RECORDS \
                    --combinedEntities=$DATA_OPERATORS_COMBINED

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord" $SPARK_JOBS_JAR \
                    --inputFile=$DATA_OPERATORS_COMBINED \
                    --outputFile=$DATA_OPERATORS_INTEGRATED_OUTPUT

# END OPERATORS INTEGRATION TEST

# START CONTACTPERSONS INTEGRATION TEST

ARG DATA_CONTACTPERSONS_INTEGRATED_INPUT=${DATA_ROOT_DIR}input/integrated/contactpersons
ARG DATA_CONTACTPERSONS_RAW=$RAW_CONTACTPERSONS_INPUT_PATH*.csv
ARG DATA_CONTACTPERSONS_INGESTED=${DATA_ROOT_DIR}ingested/common/contactpersons.parquet
ARG DATA_CONTACTPERSONS_INTEGRATED_OUTPUT=${DATA_ROOT_DIR}output/integrated/contactpersons
ARG DATA_CONTACTPERSONS_GATHERED=${DATA_ROOT_DIR}intermediate/contactpersons_gathered.parquet
ARG DATA_CONTACTPERSONS_PRE_PROCESSED=${DATA_ROOT_DIR}intermediate/contactpersons_pre_processed.parquet
ARG DATA_CONTACTPERSONS_EXACT_MATCHES=${DATA_ROOT_DIR}intermediate/contactpersons_exact_matches.parquet
ARG DATA_CONTACTPERSONS_UNMATCHED_INTEGRATED=${DATA_ROOT_DIR}intermediate/contactpersons_unmatched_integrated.parquet
ARG DATA_CONTACTPERSONS_UNMATCHED_DELTA=${DATA_ROOT_DIR}intermediate/contactpersons_unmatched_delta.parquet
ARG DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA_INTEGRATED=${DATA_ROOT_DIR}intermediate/contactpersons_fuzzy_matched_delta_integrated.parquet
ARG DATA_CONTACTPERSONS_DELTA_LEFT_OVERS=${DATA_ROOT_DIR}intermediate/contactpersons_delta_left_overs.parquet
ARG DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA=${DATA_ROOT_DIR}intermediate/contactpersons_fuzzy_matched_delta.parquet
ARG DATA_CONTACTPERSONS_DELTA_GOLDEN_RECORDS=${DATA_ROOT_DIR}intermediate/contactpersons_delta_golden_records.parquet
ARG DATA_CONTACTPERSONS_COMBINED=${DATA_ROOT_DIR}intermediate/contactpersons_combined.parquet
ARG DATA_CONTACTPERSONS_UPDATED_REFERENCES=${DATA_ROOT_DIR}intermediate/contactpersons_updated_references.parquet

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.ingest.initial.ContactPersonEmptyIntegratedWriter" $SPARK_JOBS_JAR \
                    --outputFile=$DATA_CONTACTPERSONS_INTEGRATED_INPUT

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.ingest.common.ContactPersonConverter" $SPARK_JOBS_JAR \
                    --inputFile=$DATA_CONTACTPERSONS_RAW \
                    --outputFile=$DATA_CONTACTPERSONS_INGESTED \
                    --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.ingest.GatherJob" $SPARK_JOBS_JAR \
                    --input=$DATA_CONTACTPERSONS_INGESTED \
                    --output=$DATA_CONTACTPERSONS_GATHERED

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.merging.ContactPersonPreProcess" $SPARK_JOBS_JAR \
                    --integratedInputFile=$DATA_CONTACTPERSONS_INTEGRATED_INPUT \
                    --deltaInputFile=$DATA_CONTACTPERSONS_GATHERED \
                    --deltaPreProcessedOutputFile=$DATA_CONTACTPERSONS_PRE_PROCESSED

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.merging.ContactPersonIntegratedExactMatch" $SPARK_JOBS_JAR \
                    --integratedInputFile=$DATA_CONTACTPERSONS_INGESTED \
                    --deltaInputFile=$DATA_CONTACTPERSONS_PRE_PROCESSED \
                    --matchedExactOutputFile=$DATA_CONTACTPERSONS_EXACT_MATCHES \
                    --unmatchedIntegratedOutputFile=$DATA_CONTACTPERSONS_UNMATCHED_INTEGRATED \
                    --unmatchedDeltaOutputFile=$DATA_CONTACTPERSONS_UNMATCHED_DELTA

RUN $SPARK_SUBMIT   --py-files=$SPARK_JOBS_EGG $PYTHON_DELTA_CONTACTS \
                    --integrated_input_path=$DATA_CONTACTPERSONS_UNMATCHED_INTEGRATED \
                    --ingested_daily_input_path=$DATA_CONTACTPERSONS_UNMATCHED_DELTA \
                    --updated_integrated_output_path=$DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA_INTEGRATED \
                    --unmatched_output_path=$DATA_CONTACTPERSONS_DELTA_LEFT_OVERS \
                    --country_code="DE"

RUN $SPARK_SUBMIT   --py-files=$SPARK_JOBS_EGG $PYTHON_MATCH_CONTACTS \
                    --input_file=$DATA_CONTACTPERSONS_DELTA_LEFT_OVERS \
                    --output_path=$DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA \
                    --country_code="DE"

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner" $SPARK_JOBS_JAR \
                    --matchingInputFile=$DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA \
                    --entityInputFile=$DATA_CONTACTPERSONS_DELTA_LEFT_OVERS \
                    --outputFile=$DATA_CONTACTPERSONS_DELTA_GOLDEN_RECORDS

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.combining.ContactPersonCombineExactAndFuzzyMatches" $SPARK_JOBS_JAR \
                    --contactPersonExactMatchedInputFile=$DATA_CONTACTPERSONS_EXACT_MATCHES \
                    --contactPersonFuzzyMatchedDeltaIntegratedInputFile=$DATA_CONTACTPERSONS_FUZZY_MATCHED_DELTA_INTEGRATED \
                    --contactPersonsDeltaGoldenRecordsInputFile=$DATA_CONTACTPERSONS_DELTA_GOLDEN_RECORDS \
                    --contactPersonsCombinedOutputFile=$DATA_CONTACTPERSONS_COMBINED

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.merging.ContactPersonReferencing" $SPARK_JOBS_JAR \
                    --combinedInputFile=$DATA_CONTACTPERSONS_COMBINED \
                    # TODO which integrated operators to use here? currently consuming the result of the operators test
                    --operatorInputFile=$DATA_OPERATORS_INTEGRATED_OUTPUT \
                    --outputFile=$DATA_CONTACTPERSONS_UPDATED_REFERENCES

RUN $SPARK_SUBMIT   --class="com.unilever.ohub.spark.merging.ContactPersonUpdateGoldenRecord" $SPARK_JOBS_JAR \
                    --inputFile=$DATA_CONTACTPERSONS_UPDATED_REFERENCES \
                    --outputFile=$DATA_CONTACTPERSONS_INTEGRATED_OUTPUT

# END CONTACTPERSONS INTEGRATION TEST
