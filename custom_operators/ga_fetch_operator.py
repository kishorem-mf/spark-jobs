import logging

from datetime import date, timedelta

from airflow.models import BaseOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

class GAFetchOperator(BaseOperator):
    country_codes = dict(
        AU=149299102,
        NZ=149386192,
        BE=136496201,
        FR=136417566,
        NL=136443158,
        #CN=,
        AT=136478148,
        DE=136487004,
        CH=136472077,
        #IN=,
        IL=149664234,
        GR=149621988,
        IT=149555300,
        MQ=155123886,
        LK=159477256,
        PK=159465213,
        SA=149449826,
        HK=149656154,
        TW=149647289,
        #KR=,
        CA=136493502,
        US=136408293,
        CZ=149431770,
        SK=155336641,
        EE=163567408,
        PL=149439115,
        CO=149633268,
        MX=149602702,
        #LA=,
        #DK=,
        FI=161738564,
        NO=161745261,
        #SE=,
        PT=149305761,
        RU=149644884,
        ZA=136119346,
        ID=142974636,
        MY=149419183,
        #PH=,
        SG=149358335,
        TH=149424309,
        VN=152930457,
        BG=159483761,
        HU=155330595,
        RO=155294811,
        AR=162357462,
        BR=142986451,
        CL=161669630,
        ES=136477925,
        TR=149299194,
        IE=162648003,
        GB=136489308,
    )

    def to_datetime(self, date_string):
        return date(int(date_string[0:4]), int(date_string[5:6]), int(date_string[7:8]))

    """
    Fetches Google Analytics data from BigQuery for given country codes and given timeframe and stores it as AVRO

    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param country_codes: am array of country codes to fetch for.
    :type country_codes: [string]
    :param start_date: the first date to fetch for in string form, formatted as YYYMMDD.
    :type start_date: string
    :param end_date: the last date to fetch for in string form, formatted as YYYMMDD.
    :type end_date: string
    :param destination_folder: destination folder to write the AVRO files into
    :type destination_folder: string
    """
    def __init__(self,
                bigquery_conn_id,
                country_codes,
                start_date,
                end_date,
                destination_folder,
                *args,
                **kwargs):
        super(GAFetchOperator, self).__init__(*args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id
        self.country_codes = country_codes
        self.start_date = self.to_datetime(start_date)
        self.end_date = self.to_datetime(end_date)
        self.destination_folder = destination_folder

    def execute(self, context):
        for country_code in self.country_codes:
            try :
                ga_coutry_code = GAFetchOperator.country_codes.get(country_code)
            except:
                logging.error('No GA code available for country code: ' + country_code)
                return

            date_delta = self.end_date - self.start_date
            for i in range(date_delta.days + 1):
                working_date = self.start_date + timedelta(days=i)
                working_date_fmt = working_date.__format__('YYYYMMDD')
                working_date_iso = working_date.isoformat()

                ga_dataset = ga_coutry_code + '.ga_sessions_' + working_date_fmt
                destination_uri = self.destination_folder + '/DATE=' + working_date_iso + '/COUNTRY=' + country_code + '/ga_sessions.avro'

                bq_operator = BigQueryToCloudStorageOperator(
                    source_project_dataset_table=ga_dataset,
                    destination_cloud_storage_uris=destination_uri,
                    compression='NONE',
                    export_format='AVRO',
                    field_delimiter=',',
                    print_header=True,
                    bigquery_conn_id=self.bigquery_conn_id,
                    delegate_to=None)

                bq_operator.execute(context)

