ARG BASEIMAGE=ohub2docker.azurecr.io/scalaspark:latest

FROM $BASEIMAGE

## artefacts location
ARG ARTEFACTS_DIR=/usr/local/artefacts/
RUN mkdir $ARTEFACTS_DIR

ARG SPARK_JOBS_ARTEFACTS_DIR=/usr/local/artefacts/sparkjobs
RUN mkdir -p $SPARK_JOBS_ARTEFACTS_DIR

ARG NAME_MATCHING_EGG_ARTEFACTS_DIR=/usr/local/artefacts/name-matching/egg
RUN mkdir -p $NAME_MATCHING_EGG_ARTEFACTS_DIR

ARG NAME_MATCHING_MAIN_ARTEFACTS_DIR=/usr/local/artefacts/name-matching/main
RUN mkdir -p $NAME_MATCHING_MAIN_ARTEFACTS_DIR

# copy test input data
ARG DATA_ROOT_DIR=/usr/local/data/
COPY data $DATA_ROOT_DIR

# copy data pipelines
ARG DATA_PIPELINE_SCRIPTS_DIR=/usr/local/pipelines/
RUN mkdir -p $DATA_PIPELINE_SCRIPTS_DIR
COPY pipelines $DATA_PIPELINE_SCRIPTS_DIR
RUN find $DATA_PIPELINE_SCRIPTS_DIR -type f -name "*.sh" -exec chmod +x {} +

# copy test scripts
ARG TEST_SCRIPTS_DIR=/usr/local/it-test/
RUN mkdir -p $TEST_SCRIPTS_DIR
COPY it-test $TEST_SCRIPTS_DIR

# start spark cluster with 2 worker nodes
RUN start-master.sh
RUN start-slave.sh spark://spark:7077
RUN start-slave.sh spark://spark:7077
