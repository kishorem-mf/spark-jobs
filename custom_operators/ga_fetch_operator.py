import logging

import os
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

FILE_NAME = 'ga_data.avro'


class GAToGSOperator(BaseOperator):
    """
    Fetches Google Analytics data from BigQuery for given country
     codes and date and stores it as AVRO.

    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param country_codes: a map from country codes to fetch for
     to their corresponding GA code.
    :type country_codes: [dict]
    :param date: the first date to fetch for in string form,
                       formatted as YYYMMDD.
    :type date: str
    :param destination: destination folder to write the
     AVRO files into
    :type destination: string
    """

    template_fields = ('date', 'destination')

    def __init__(self,
                 bigquery_conn_id,
                 country_codes,
                 date,
                 destination,
                 *args,
                 **kwargs):
        super(GAToGSOperator, self).__init__(*args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        self.country_codes = country_codes
        self.date = date
        self.destination = destination

    def fetch_for_date(self,
                       context,
                       bigquery_conn_id,
                       country_code,
                       ga_country_code,
                       working_date,
                       destination):
        ga_dataset = '{country_code}.ga_sessions_{working_date}'.format(country_code=ga_country_code,
                                                                        working_date=working_date.replace('-', ''))
        destination = '{dest}/PARTITION_DATE={date}/COUNTRY_CODE={country}/{fn}'.format(dest=destination,
                                                                                        date=working_date,
                                                                                        country=country_code,
                                                                                        fn=FILE_NAME)

        bq_operator = BigQueryToCloudStorageOperator(
            task_id='to_gs',
            source_project_dataset_table=ga_dataset,
            destination_cloud_storage_uris=[destination],
            compression='NONE',
            export_format='AVRO',
            field_delimiter=',',
            print_header=True,
            bigquery_conn_id=bigquery_conn_id)

        bq_operator.execute(context)

    def fetch_for_country(self,
                          context,
                          bigquery_conn_id,
                          country_code,
                          date,
                          destination):
        try:
            ga_country_code = self.country_codes[country_code]
        except Exception as e:
            logging.error(
                'No GA code available for country code: {}'.format(country_code), e)
            return

        self.fetch_for_date(context,
                            bigquery_conn_id,
                            country_code,
                            ga_country_code,
                            date,
                            destination)

    def execute(self, context):
        for country_code in self.country_codes.keys():
            self.fetch_for_country(context,
                                   self.bigquery_conn_id,
                                   country_code,
                                   self.date,
                                   self.destination)


class GSToLocalOperator(BaseOperator):
    template_fields = ('date',)

    @apply_defaults
    def __init__(self,
                 path,
                 date,
                 bucket,
                 path_in_bucket,
                 gcp_conn_id,
                 country_codes,
                 *args,
                 **kwargs):
        super(GSToLocalOperator, self).__init__(*args, **kwargs)
        self.path = path
        self.date = date
        self.country_codes = country_codes
        self.bucket = bucket
        self.path_in_bucket = path_in_bucket
        self.gcp_conn_id = gcp_conn_id

    def download_file(self,
                      context,
                      connection_id,
                      fn,
                      bucket,
                      obj):
        """Download a file to local storage"""
        self.log.info('Downloading {obj} to {fn}'.format(**locals()))
        operator = GoogleCloudStorageDownloadOperator(
            task_id='download',
            bucket=bucket,
            object=obj,
            filename=fn,
            google_cloud_storage_conn_id=connection_id)

        dir = '/'.join(fn.split('/')[:-1])
        if not os.path.exists(dir):
            os.makedirs(dir)

        operator.execute(context)

    def execute(self, context):
        for country_code in self.country_codes.keys():
            obj = '{}/PARTITION_DATE={}/COUNTRY_CODE={}/{}'.format(self.path_in_bucket, self.date, country_code,
                                                                   FILE_NAME)
            file_path = self.path + 'PARTITION_DATE={}/COUNTRY_CODE={}/{}'.format(self.date, country_code, FILE_NAME)
            self.download_file(context, self.gcp_conn_id, file_path, self.bucket, obj)


class LocalGAToWasbOperator(BaseOperator):
    template_fields = ('date',)

    @apply_defaults
    def __init__(self,
                 path,
                 container_name,
                 blob_path,
                 date,
                 country_codes,
                 wasb_conn_id='wasb_default',
                 load_options=None,
                 *args,
                 **kwargs):
        super(LocalGAToWasbOperator, self).__init__(*args, **kwargs)
        if load_options is None:
            load_options = {}
        self.path = path
        self.date = date
        self.country_codes = country_codes
        self.container_name = container_name
        self.blob_path = blob_path
        self.wasb_conn_id = wasb_conn_id
        self.load_options = load_options

    def upload_file(self,
                    hook,
                    blob_name,
                    container_name,
                    file_path):
        self.log.info('Uploading {file_path} to wasb://{container_name} as {blob_name}'.format(**locals()))
        hook.load_file(file_path, container_name, blob_name, **self.load_options)

    def execute(self, context):
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        for country_code in self.country_codes.keys():
            blob_name = self.blob_path + 'PARTITION_DATE={}/COUNTRY_CODE={}/{}'.format(self.date, country_code, FILE_NAME)
            file_path = self.path + 'PARTITION_DATE={}/COUNTRY_CODE={}/{}'.format(self.date, country_code, FILE_NAME)
            self.upload_file(hook, blob_name, self.container_name, file_path)
