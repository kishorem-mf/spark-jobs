from datetime import timedelta
from config import email_addresses, slack_on_databricks_failure_callback

operator_country_codes = ['AD', 'AE', 'AF', 'AR', 'AT', 'AU', 'AZ', 'BD', 'BE', 'BG', 'BH', 'BO', 'BR', 'CA', 'CH',
                          'CL', 'CN', 'CO', 'CR', 'CZ', 'DE', 'DK', 'DO', 'EC', 'EE', 'EG', 'ES', 'FI', 'FR', 'GB',
                          'GE', 'GR', 'GT', 'HK', 'HN', 'HU', 'ID', 'IE', 'IL', 'IN', 'IR', 'IT', 'JO', 'KR', 'KW',
                          'LB', 'LK', 'LT', 'LU', 'LV', 'MA', 'MM', 'MO', 'MV', 'MX', 'MY', 'NI', 'NL', 'NO', 'NU',
                          'NZ', 'OM', 'PA', 'PE', 'PH', 'PK', 'PL', 'PT', 'QA', 'RO', 'RU', 'SA', 'SE', 'SG', 'SK',
                          'SV', 'TH', 'TR', 'TW', 'US', 'VE', 'VN', 'ZA']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': email_addresses,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'pool': 'ohub_pool',
    'on_failure_callback': slack_on_databricks_failure_callback
}

cluster_id = '0503-091728-vouch458'
databricks_conn_id = 'databricks_azure'

jar = 'dbfs:/libraries/ohub/spark-jobs-assembly-0.2.0.jar'
egg = 'dbfs:/libraries/name_matching/string_matching.egg'

dbfs_root_bucket = 'dbfs:/mnt/ohub_data/'
raw_bucket = dbfs_root_bucket + 'raw/{schema}/{date}/file_interface/*.csv'
ingested_bucket = dbfs_root_bucket + 'demo/ingested/{date}/{fn}.parquet'
intermediate_bucket = dbfs_root_bucket + 'demo/intermediate/{date}/{fn}.parquet'
integrated_bucket = dbfs_root_bucket + 'demo/integrated/{date}/{fn}.parquet'
export_bucket = dbfs_root_bucket + 'demo/export/{date}/{fn}'

wasb_root_bucket = 'dbfs:/mnt/ohub_data/'
wasb_raw_bucket = wasb_root_bucket + 'raw/{schema}/{date}/file_interface/*.csv'
wasb_ingested_bucket = wasb_root_bucket + 'demo/ingested/{date}/{fn}.parquet'
wasb_intermediate_bucket = wasb_root_bucket + 'demo/intermediate/{date}/{fn}.parquet'
wasb_integrated_bucket = wasb_root_bucket + 'demo/integrated/{date}/{fn}.parquet'
wasb_export_bucket = wasb_root_bucket + 'demo/export/{date}/{fn}'
