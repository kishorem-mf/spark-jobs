ARG BASEIMAGE=ohub2docker.azurecr.io/docker-airflow:latest
# todo: set back to fixed version for vsts
# Ignore "always hard-code base image version" because we parameterize this to make both laptop and bitbucket builds work.
# hadolint ignore=DL3006
FROM $BASEIMAGE

# Install custom library
COPY setup.py /root/ohub/setup.py
COPY ohub /root/ohub/ohub
# Ignore "always hard-code pip versions" because we install the source code that's defined in this very module
# hadolint ignore=DL3013
RUN pip install /root/ohub

# Install DAGs
# match with AIRFLOW__CORE__DAGS_FOLDER
COPY dags /root/airflow/dags/dags
ENV PYTHONPATH=/airflow
