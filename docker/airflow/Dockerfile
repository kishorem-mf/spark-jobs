FROM continuumio/miniconda:4.5.4

ENV PYTHONDONTWRITEBYTECODE 1

RUN set -ex \
    && apt-get update \
    && apt-get install --no-install-recommends -y gcc \
    && pip install --no-cache-dir apache-airflow[crypto,postgres,azure]==1.9.0 \
    && rm -rf /var/lib/apt/lists/*

# Fix replacing colons in filenames by dots because Azure File Share cannot handle colons in filenames
# Fix available in Airflow 1.10 https://issues.apache.org/jira/browse/AIRFLOW-2539
RUN sed -i.bak 's/{{ ts }}/{{ ts | replace(":",".") }}/g' /opt/miniconda3/lib/python3.6/site-packages/airflow/config_templates/airflow_local_settings.py

# latest logs dir is a symlink which is not supported by Azure File Share. If this fails (which it will), Airflow will
# call self.log which doesn't exist. Replace by logging.warning. You will see lots of this in the logs:
# [2018-07-19 07:23:35,051] {file_processor_handler.py:112} WARNING - /root/airflow/logs/scheduler/latest already exists as a dir/file. Skip creating symlink.
RUN sed -i.bak 's/self.log.warning/logging.warning/g' /opt/miniconda3/lib/python3.6/site-packages/airflow/utils/log/file_processor_handler.py

VOLUME ["/root/airflow/logs", "/root/airflow/dags", "/root/airflow/plugins"]
WORKDIR /usr/app

EXPOSE 8080
