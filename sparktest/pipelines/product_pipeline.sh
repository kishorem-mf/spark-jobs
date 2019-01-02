#!/usr/bin/env bash

echo "running product data pipeline"

ARTEFACTS_DIR="/usr/local/artefacts/"
DATA_ROOT_DIR="/usr/local/data/"

RAW_INPUT_PATH="${DATA_ROOT_DIR}raw/products/*.csv"

SPARK_JOBS_JAR=${ARTEFACTS_DIR}sparkjobs/spark-jobs-assembly-0.2.0.jar

DATA_INTEGRATED_INPUT="${DATA_ROOT_DIR}input/integrated/products"
DATA_INGESTED="${DATA_ROOT_DIR}ingested/common/products.parquet"
DATA_PRE_PROCESSED="${DATA_ROOT_DIR}intermediate/products_pre_processed.parquet"
DATA_INTEGRATED_OUTPUT="${DATA_ROOT_DIR}output/integrated/products"

spark-submit    --class="com.unilever.ohub.spark.ingest.initial.ProductEmptyIntegratedWriter" ${SPARK_JOBS_JAR} \
                --outputFile=${DATA_INTEGRATED_INPUT}

spark-submit    --class="com.unilever.ohub.spark.ingest.common.ProductConverter" ${SPARK_JOBS_JAR} \
                --inputFile=${RAW_INPUT_PATH} \
                --outputFile=${DATA_INGESTED} \
                --fieldSeparator=";" --strictIngestion="false" --deduplicateOnConcatId="true"

spark-submit    --class="com.unilever.ohub.spark.merging.ProductPreProcess" ${SPARK_JOBS_JAR} \
                --integratedInputFile=${DATA_INTEGRATED_INPUT} \
                --deltaInputFile=${DATA_INGESTED} \
                --deltaPreProcessedOutputFile=${DATA_PRE_PROCESSED}

spark-submit    --class="com.unilever.ohub.spark.merging.ProductMerging" ${SPARK_JOBS_JAR} \
                --productsInputFile=${DATA_PRE_PROCESSED} \
                --previousIntegrated=${DATA_INTEGRATED_INPUT} \
                --outputFile=${DATA_INTEGRATED_OUTPUT}
