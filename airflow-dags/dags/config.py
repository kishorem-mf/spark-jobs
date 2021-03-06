from datetime import datetime, timedelta
from airflow.configuration import conf
from ohub.utils.airflow import slack_on_databricks_failure_callback

email_addresses = ["ohub-team@ufs.com"]

dag_default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "email": email_addresses,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": slack_on_databricks_failure_callback,
}

ohub_entities = {
    "operators": {"spark_class": "Operator"},
    "contactpersons": {"spark_class": "ContactPerson"},
    "orders": {"spark_class": "Order"},
    "orderlines": {"spark_class": "OrderLine"},
    "products": {"spark_class": "Product"},
    "subscriptions": {"spark_class": "Subscription"}
}

if conf.getboolean('core', 'unit_test_mode'):
    start_date_first = datetime(2018, 7, 25)
    start_date_delta = datetime(2018, 7, 26)
    ohub_country_codes = ["DE"]
else:
    start_date_first = datetime(2018, 8, 1)
    start_date_delta = datetime(2018, 8, 2)
    ohub_country_codes = [
        "AD",
        "AE",
        "AF",
        "AR",
        "AT",
        "AU",
        "AZ",
        "BD",
        "BE",
        "BG",
        "BH",
        "BO",
        "BR",
        "CA",
        "CH",
        "CL",
        "CN",
        "CO",
        "CR",
        "CZ",
        "DE",
        "DK",
        "DO",
        "EC",
        "EE",
        "EG",
        "ES",
        "FI",
        "FR",
        "GB",
        "GE",
        "GR",
        "GT",
        "HK",
        "HN",
        "HU",
        "ID",
        "IE",
        "IL",
        "IN",
        "IR",
        "IT",
        "JO",
        "KR",
        "KW",
        "LB",
        "LK",
        "LT",
        "LU",
        "LV",
        "MA",
        "MM",
        "MO",
        "MV",
        "MX",
        "MY",
        "NI",
        "NL",
        "NO",
        "NU",
        "NZ",
        "OM",
        "PA",
        "PE",
        "PH",
        "PK",
        "PL",
        "PT",
        "QA",
        "RO",
        "RU",
        "SA",
        "SE",
        "SG",
        "SK",
        "SV",
        "TH",
        "TR",
        "TW",
        "US",
        "VE",
        "VN",
        "ZA",
    ]


country_codes = dict(
    AU=149299102,
    NZ=149386192,
    BE=136496201,
    FR=136417566,
    NL=136443158,
    # CN=,
    AT=136478148,
    DE=136487004,
    CH=136472077,
    # IN=,
    IL=149664234,
    GR=149621988,
    IT=149555300,
    MQ=155123886,
    LK=159477256,
    PK=159465213,
    SA=149449826,
    HK=149656154,
    TW=149647289,
    # KR=,
    CA=136493502,
    # US=136408293, # often errors with not found
    CZ=149431770,
    SK=155336641,
    EE=163567408,
    PL=149439115,
    CO=149633268,
    MX=149602702,
    # LA=,
    # DK=,
    FI=161738564,
    NO=161745261,
    # SE=,
    PT=149305761,
    RU=149644884,
    ZA=136119346,
    ID=142974636,
    MY=149419183,
    PH=149403978,
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

databricks_conn_id = "databricks_default"


def cluster_config(cluster_name: str = '', large=False, test=True):
    """Returns a Databricks cluster configuration.
    Large clusters are useful for name-matching.
    Test mode reuses a cluster to save spin-up time, at the expense of separated logs.
    TODO: use large cluster just for name-matching parts, not for all other operator/cpn steps.
    """
    type_id = "Standard_D16s_v3" if large else "Standard_DS3_v2"
    return {
        **{
            "spark_version": "4.0.x-scala2.11",
            "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
            "node_type_id": type_id,
        },
        **({
            "cluster_name": "ohub " + type_id,
            "reuse_cluster": '1',   # airflow 1.9 did not yet support templating bools or ints...
        } if test else {
            "cluster_name": cluster_name,
            "reuse_cluster": '0',
        }),
        **({
            "autoscale": {"min_workers": "4", "max_workers": "12"},
            "autotermination_minutes": "30",
        } if large else {
            "num_workers": "4",
            "autotermination_minutes": "60",
        }),
    }


# interval = "@daily"
# one_day_ago = "{{ ds }}"
# two_day_ago = "{{ yesterday_ds }}"


wasb_conn_id = "azure_blob"
container_name = "prod"

spark_jobs_jar = "dbfs:/libraries/ohub/spark-jobs-assembly-0.2.0.jar"
string_matching_egg = "dbfs:/libraries/name_matching/string_matching.egg"

dbfs_root_bucket = "dbfs:/mnt/ohub_data/"
raw_bucket = dbfs_root_bucket + "raw/{schema}/{date}/{channel}/*.csv"
ingested_bucket = dbfs_root_bucket + "ingested/{date}/{channel}/{fn}.parquet"
intermediate_bucket = dbfs_root_bucket + "intermediate/{date}/{fn}.parquet"
integrated_bucket = dbfs_root_bucket + "integrated/{date}/{fn}.parquet"
export_bucket = dbfs_root_bucket + "export/{date}/{fn}"

wasb_root_bucket = "data/"
wasb_raw_container = wasb_root_bucket + "raw/{schema}/{date}/{channel}/*.csv"
wasb_ingested_container = wasb_root_bucket + "ingested/{date}/{fn}.parquet"
wasb_intermediate_container = wasb_root_bucket + "intermediate/{date}/{fn}.parquet"
wasb_integrated_container = wasb_root_bucket + "integrated/{date}/{fn}.parquet"
wasb_export_container = wasb_root_bucket + "export/{date}/{fn}"

http_root_bucket = "https://{storage_account}.blob.core.windows.net/{container}/data/"
http_raw_container = http_root_bucket + "raw/{schema}/{date}/{channel}/*.csv"
http_ingested_container = http_root_bucket + "ingested/{date}/{fn}.parquet"
http_intermediate_container = http_root_bucket + "intermediate/{date}/{fn}.parquet"
http_integrated_container = http_root_bucket + "integrated/{date}/{fn}.parquet"
http_export_container = http_root_bucket + "export/{date}/{fn}"
