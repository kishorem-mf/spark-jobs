import logging

from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

from airflow.contrib.hooks.wasb_hook import WasbHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

LOCAL_FILE_NAME = 'ga_sessions.avro'


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
    :param destination_folder: destination folder to write the
     AVRO files into
    :type destination_folder: string
    """

    template_fields = ('date', 'destination_folder')

    def __init__(self,
                 bigquery_conn_id,
                 country_codes,
                 date,
                 destination_folder,
                 *args,
                 **kwargs):
        super(GAToGSOperator, self).__init__(*args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        self.country_codes = country_codes
        self.date = date
        self.destination_folder = destination_folder

    def fetch_for_date(self,
                       context,
                       bigquery_conn_id,
                       country_code,
                       ga_country_code,
                       working_date,
                       destination_folder):
        ga_dataset = '{country_code}.ga_sessions_{working_date}'.format(country_code=ga_country_code,
                                                                        working_date=working_date)
        destination = '{dest}/DATE={date}/COUNTRY={country}/{fn}'.format(dest=destination_folder,
                                                                         date=working_date,
                                                                         country=country_code,
                                                                         fn=LOCAL_FILE_NAME)

        bq_operator = BigQueryToCloudStorageOperator(
            source_project_dataset_table=ga_dataset,
            destination_cloud_storage_uris=destination,
            compression='NONE',
            export_format='AVRO',
            field_delimiter=',',
            print_header=True,
            bigquery_conn_id=bigquery_conn_id,
            delegate_to=None)

        bq_operator.execute(context)

    def fetch_for_country(self,
                          context,
                          bigquery_conn_id,
                          country_code,
                          date,
                          destination_folder):
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
                            destination_folder)

    def execute(self, context):
        for country_code in self.country_codes.keys():
            self.fetch_for_country(context,
                                   self.bigquery_conn_id,
                                   country_code,
                                   self.date,
                                   self.destination_folder)



class LocalGAToWasbOperator(BaseOperator):
    template_fields = ('path', 'date')

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
        if load_options is None:
            load_options = {}
        self.path = path
        self.data = date
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
        self.log.info('Uploading {file} to wasb://{container_name} as {blob_name}'.format(**locals()))
        hook.load_file(file_path, self.container_name, self.blob_name, **self.load_options)

    def execute(self, context):
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        for country_code in self.country_codes.keys():
            blob_name = self.blob_path + '/DATE={}/COUNTRY={}/{}'.format(self.date, country_code, LOCAL_FILE_NAME)
            file_path = self.path + '/DATE={}/COUNTRY={}/{}'.format(self.date, country_code, LOCAL_FILE_NAME)
            self.upload_file(hook, blob_name, self.container_name, file_path)
