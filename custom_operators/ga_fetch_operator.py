import logging

from datetime import date, timedelta

from airflow.models import BaseOperator
from airflow.contrib.operators.bigquery_to_gcs import (
    BigQueryToCloudStorageOperator)


class GAFetchOperator(BaseOperator):
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
    :type date: date
    :param destination_folder: destination folder to write the
     AVRO files into
    :type destination_folder: string
    """
    def __init__(self,
                 bigquery_conn_id,
                 country_codes,
                 date,
                 destination_folder,
                 *args,
                 **kwargs):
        super(GAFetchOperator, self).__init__(*args, **kwargs)
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
        working_date_fmt = working_date.strftime('%Y%m%d')
        working_date_iso = working_date.isoformat()

        ga_dataset = """{ga_code}.ga_sessions_{date}""".format(
            ga_code=ga_country_code,
            date=working_date_fmt)
        to_fmt = """{folder}/DATE={date}/COUNTRY={country}/ga_sessions.avro"""
        destination_uri = to_fmt.format(
            folder=destination_folder,
            date=working_date_iso,
            country=country_code)

        bq_operator = BigQueryToCloudStorageOperator(
            source_project_dataset_table=ga_dataset,
            destination_cloud_storage_uris=destination_uri,
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
            ga_country_code = self.country_codes.get(country_code)
        except Exception as e:
            logging.error(
                'No GA code available for country code: ' + country_code, e)
            return

        self.fetch_for_date(context,
                                       bigquery_conn_id,
                                       country_code,
                                       ga_country_code,
                                       date,
                                       destination_folder)

    def execute(self, context):
        for country_code in self.country_codes:
            self.fetch_for_country(context,
                                   self.bigquery_conn_id,
                                   country_code,
                                   self.date,
                                   self.destination_folder)
