#!/usr/bin/env bash

echo "running order data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_ORDERS_INPUT_PATH="${DATA_ROOT_DIR}raw/orders/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_ORDERS_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/orders"
DATA_ORDERS_INGESTED="${DATA_ROOT_DIR}ingested/common/orders.parquet"
DATA_ORDERS_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/orders_pre_processed.parquet"
DATA_ORDERS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/orders"

DATA_ORDERLINES_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/orderlines"
DATA_ORDERLINES_INGESTED="${DATA_ROOT_DIR}ingested/common/orderlines.parquet"
DATA_ORDERLINES_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/orderlines_pre_processed.parquet"
DATA_ORDERLINES_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/orderlines"

DATA_OPERATORS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/operators"
DATA_CONTACTPERSONS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/contactpersons"
DATA_PRODUCTS_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/products"

echo
echo OrderEmptyIntegratedWriter
spark-submit    --class="com.unilever.ohub.spark.ingest.initial.OrderEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_ORDERS_INTEGRATED_INPUT}

echo
echo OrderLineEmptyIntegratedWriter
spark-submit    --class="com.unilever.ohub.spark.ingest.initial.OrderLineEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_ORDERLINES_INTEGRATED_INPUT}

echo
echo OrderConverter
spark-submit    --class="com.unilever.ohub.spark.ingest.common.OrderConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_ORDERS_INPUT_PATH} \
                --outputFile=${DATA_ORDERS_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo OrderLineConverter
spark-submit    --class="com.unilever.ohub.spark.ingest.common.OrderLineConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_ORDERS_INPUT_PATH} \
                --outputFile=${DATA_ORDERLINES_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

echo
echo OrderPreProcess
spark-submit    --class="com.unilever.ohub.spark.merging.OrderPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_ORDERS_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_ORDERS_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_ORDERS_PRE_PROCESSED}

echo
echo OrderLinePreProcess
spark-submit    --class="com.unilever.ohub.spark.merging.OrderLinePreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_ORDERLINES_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_ORDERLINES_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_ORDERLINES_PRE_PROCESSED}

echo
echo OrderMerging
spark-submit    --class="com.unilever.ohub.spark.merging.OrderMerging" ${SPARK_JOBS_JAR} \
                --orderInputFile=${DATA_ORDERS_PRE_PROCESSED} \
                --previousIntegrated=${DATA_ORDERS_INTEGRATED_INPUT} \
                --contactPersonInputFile=${DATA_CONTACTPERSONS_INTEGRATED_OUTPUT} \
                --operatorInputFile=${DATA_OPERATORS_INTEGRATED_OUTPUT} \
                --outputFile=${DATA_ORDERS_INTEGRATED_OUTPUT}

echo
echo OrderLineMerging
spark-submit    --class="com.unilever.ohub.spark.merging.OrderLineMerging" ${SPARK_JOBS_JAR} \
                --orderLineInputFile=${DATA_ORDERLINES_PRE_PROCESSED} \
                --previousIntegrated=${DATA_ORDERLINES_INTEGRATED_INPUT} \
                --productsIntegrated=${DATA_PRODUCTS_INTEGRATED_OUTPUT} \
                --outputFile=${DATA_ORDERLINES_INTEGRATED_OUTPUT}
