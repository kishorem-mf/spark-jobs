ARG BASEIMAGE=ulohubimages.azurecr.io/airflow_base:109
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
COPY dags /airflow/dags
ENV PYTHONPATH=/airflow
