import os
import pytest
import requests_mock
from airflow import AirflowException

from dags.custom_operators import databricks_functions

API_URL = 'https://unittest.example.com'
CONN_ID = 'unittest-databricks'

os.environ['AIRFLOW_CONN_' + CONN_ID.upper()] = API_URL


@pytest.fixture()
def m():
    with requests_mock.Mocker() as m:
        yield m


def test_find_cluster_id_should_raise_if_no_cluster_found(m):
    m.get(API_URL + '/api/2.0/clusters/list', json={"clusters": []})
    with pytest.raises(AirflowException):
        databricks_functions.find_cluster_id("a_name", CONN_ID)


def test_find_cluster_id_should_find_running_cluster(m):
    m.get(API_URL + '/api/2.0/clusters/list', json={"clusters": [
        {'cluster_name': 'a_name', 'state': 'TERMINATED', 'cluster_id': 'ignore_1'},
        {'cluster_name': 'b_name', 'state': 'RUNNING', 'cluster_id': 'ignore_2'},
        {'cluster_name': 'a_name', 'state': 'RUNNING', 'cluster_id': 'match'},
    ]})
    assert "match" == databricks_functions.find_cluster_id("a_name", CONN_ID)


def test_find_cluster_id_should_return_first_if_multiple_clusters_match(m):
    m.get(API_URL + '/api/2.0/clusters/list', json={"clusters": [
        {'cluster_name': 'a_name', 'state': 'RUNNING', 'cluster_id': 'match_1'},
        {'cluster_name': 'a_name', 'state': 'RUNNING', 'cluster_id': 'match_2'},
    ]})
    assert "match_1" == databricks_functions.find_cluster_id("a_name", CONN_ID)


def test_get_cluster_status(m):
    m.get(API_URL + '/api/2.0/clusters/get', json=full_cluster_status_example)
    assert databricks_functions.get_cluster_status("a_name", CONN_ID) == 'RUNNING'


full_cluster_status_example = {
    "autoscale": {
        "max_workers": 15,
        "min_workers": 2
    },
    "autotermination_minutes": 0,
    "aws_attributes": {
        "availability": "SPOT_WITH_FALLBACK",
        "ebs_volume_count": 1,
        "ebs_volume_size": 100,
        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
        "first_on_demand": 2,
        "instance_profile_arn": "foo",
        "spot_bid_price_percent": 50,
        "zone_id": "eu-west-1b"
    },
    "cluster_cores": 48.0,
    "cluster_created_by": "THIRD_PARTY",
    "cluster_id": "bar",
    "cluster_log_conf": {
        "s3": {
            "canned_acl": "bucket-owner-full-control",
            "destination": "s3://foobar",
            "enable_encryption": True,
            "region": "us-west-2"
        }
    },
    "cluster_log_status": {
        "last_attempted": 1502793860316,
        "last_exception": "AmazonS3Exception: The bucket is in this region: null."
    },
    "cluster_memory_mb": 186368,
    "cluster_name": "foo_name",
    "creator_user_name": "bar_creator",
    "custom_tags": {
        "CostCenter": "international-operations",
        "Environment": "env_prod",
        "GroupName": "Dave",
        "Provisioner": "Mustaine",
        "Stage": "prod",
        "Tenant": "secret"
    },
    "default_tags": {
        "ClusterId": "some_id_random_hash",
        "ClusterName": "some_name",
        "Creator": "dave_mustane",
        "Vendor": "Databricks"
    },
    "driver": {
        "host_private_ip": "1.0.0.0",
        "instance_id": "i-666",
        "node_aws_attributes": {
            "is_spot": False
        },
        "node_id": "hashihash",
        "private_ip": "2.0.0.0",
        "public_dns": "google.com",
        "start_timestamp": 1502790246809
    },
    "driver_node_type_id": "r4.4xlarge",
    "enable_elastic_disk": False,
    "executors": [
        {
            "host_private_ip": "3.0.0.0",
            "instance_id": "i-2",
            "node_aws_attributes": {
                "is_spot": False
            },
            "node_id": "random_node",
            "private_ip": "4.0.0.0",
            "public_dns": "again.google.com",
            "start_timestamp": 1502790246761
        },
    ],
    "jdbc_port": 10000,
    "last_activity_time": 1502291317824,
    "last_state_loss_time": 1502790278769,
    "node_type_id": "c4.4xlarge",
    "spark_conf": {
        "#": "spark.hadoop.fs.s3a.stsAssumeRole.arn ${sts-arn}",
        "spark.hadoop.fs.s3.impl": "com.databricks.s3a.S3AFileSystem",
        "spark.hadoop.javax.jdo.option.ConnectionDriverName": "com.mysql.jdbc.Driver",
        "spark.hadoop.javax.jdo.option.ConnectionPassword": "pass",
        "spark.hadoop.javax.jdo.option.ConnectionURL": "jdbc:mysql://foo",
        "spark.hadoop.javax.jdo.option.ConnectionUserName": "databricks",
        "spark.io.compression.codec": "snappy",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.hive.metastore.jars": "builtin",
        "spark.sql.hive.metastore.version": "1.2.1"
    },
    "spark_context_id": 8404907203906325937,
    "spark_version": "3.0.x-scala2.11",
    "start_time": 1501677397652,
    "state": "RUNNING",
    "state_message": "",
    "terminated_time": 0
}
