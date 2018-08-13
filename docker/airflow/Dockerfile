ARG BASEIMAGE=ulohubimages.azurecr.io/airflow_base:109
FROM $BASEIMAGE

# Install custom library
COPY setup.py /root/ohub/setup.py
COPY ohub /root/ohub/ohub
RUN pip install /root/ohub

# Install DAGs
COPY dags /airflow/dags
ENV PYTHONPATH=/airflow
