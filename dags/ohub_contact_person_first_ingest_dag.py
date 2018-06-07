from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator, SFTPOperation
from airflow.operators.bash_operator import BashOperator

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from custom_operators.file_from_wasb import FileFromWasbOperator
from custom_operators.external_task_sensor_operator import ExternalTaskSensorOperator
from ohub_dag_config import \
    default_args, databricks_conn_id, jar, container_name, \
    ingested_bucket, intermediate_bucket, integrated_bucket, export_bucket, \
    wasb_export_container, create_cluster, default_cluster_config, terminate_cluster, ingest_task, \
    fuzzy_matching_tasks, postgres_config

default_args.update(
    {
        'start_date': datetime(2018, 6, 3),
        'pool': 'ohub_contactpersons_pool'
    }
)
interval = '@once'

schema = 'contactpersons'
cluster_name = "ohub_contactpersons_initial_load_{{ds}}"

with DAG('ohub_{}_first_ingest'.format(schema), default_args=default_args,
         schedule_interval=interval) as dag:
    contactpersons_create_cluster = create_cluster('{}_create_clusters'.format(schema),
                                                   default_cluster_config(cluster_name))
    contactpersons_terminate_cluster = terminate_cluster('{}_terminate_cluster'.format(schema), cluster_name)

    contact_persons_file_interface_to_parquet = ingest_task(
        schema=schema,
        channel='file_interface',
        clazz="com.unilever.ohub.spark.tsv2parquet.file_interface.ContactPersonConverter",
        field_separator=u"\u2030",
        cluster_name=cluster_name
    )

    contact_persons_exact_match = DatabricksSubmitRunOperator(
        task_id="{}_exact_match".format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonExactMatcher",
            'parameters': ['--inputFile', ingested_bucket.format(date='{{ds}}',
                                                                 fn=schema,
                                                                 channel='file_interface'),
                           '--exactMatchOutputFile', intermediate_bucket.format(date='{{ds}}',
                                                                                fn='{}_exact_match'.format(schema)),
                           '--leftOversOutputFile', intermediate_bucket.format(date='{{ds}}',
                                                                               fn='{}_left_overs'.format(
                                                                                   schema))] + postgres_config
        }
    )

    begin_fuzzy_matching = BashOperator(
        task_id='{}_start_fuzzy_matching'.format(schema),
        bash_command='echo "start fuzzy matching"',
    )

    matching_tasks = fuzzy_matching_tasks(
        schema=schema,
        cluster_name=cluster_name,
        match_py='dbfs:/libraries/name_matching/match_contacts.py',
        ingested_input=intermediate_bucket.format(date='{{ds}}', fn='{}_left_overs'.format(schema)),
    )

    end_fuzzy_matching = BashOperator(
        task_id='{}_end_fuzzy_matching'.format(schema),
        bash_command='echo "end fuzzy matching"',
    )

    join_contact_persons = DatabricksSubmitRunOperator(
        task_id='{}_merge'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date='{{ds}}', fn='{}_matched'.format(schema)),
                           '--entityInputFile', intermediate_bucket.format(date='{{ds}}',
                                                                           fn='{}_left_overs'.format(schema)),
                           '--outputFile', intermediate_bucket.format(date='{{ds}}', fn=schema)] + postgres_config
        }
    )

    contact_person_combining = DatabricksSubmitRunOperator(
        task_id='{}_combining'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],

        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombining",
            'parameters': ['--integratedUpdated', intermediate_bucket.format(date='{{ds}}',
                                                                             fn='{}_exact_match'.format(schema)),
                           '--newGolden', intermediate_bucket.format(date='{{ds}}',
                                                                     fn=schema,
                                                                     channel='*'),
                           '--combinedEntities', intermediate_bucket.format(date='{{ds}}',
                                                                            fn='{}_combined'.format(schema))]
        }
    )

    operators_integrated_sensor = ExternalTaskSensorOperator(
        task_id='operators_integrated_sensor',
        external_dag_id='ohub_operators_first_ingest',
        external_task_id='operators_join'
    )

    contact_person_referencing = DatabricksSubmitRunOperator(
        task_id='{}_referencing'.format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],

        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonReferencing",
            'parameters': ['--combinedInputFile', intermediate_bucket.format(date='{{ds}}',
                                                                             fn='{}_combined'.format(schema)),
                           '--operatorInputFile', integrated_bucket.format(date='{{ds}}',
                                                                           fn='operators',
                                                                           channel='*'),
                           '--outputFile', integrated_bucket.format(date='{{ds}}', fn=schema)]
        }
    )

    cp_file = 'acm/UFS_RECIPIENTS_{{ds_nodash}}000000.csv'

    contact_persons_to_acm = DatabricksSubmitRunOperator(
        task_id="{}_to_acm".format(schema),
        cluster_name=cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.acm.ContactPersonAcmConverter",
            'parameters': ['--inputFile', integrated_bucket.format(date='{{ds}}', fn=schema),
                           '--outputFile', export_bucket.format(date='{{ds}}', fn=cp_file)]
        }
    )

    tmp_file = '/tmp/' + cp_file

    contact_persons_acm_from_wasb = FileFromWasbOperator(
        task_id='{}_acm_from_wasb'.format(schema),
        file_path=tmp_file,
        container_name=container_name,
        wasb_conn_id='azure_blob',
        blob_name=wasb_export_container.format(date='{{ds}}', fn=cp_file)
    )

    contact_person_ftp_to_acm = SFTPOperator(
        task_id='{}_ftp_to_acm'.format(schema),
        local_filepath=tmp_file,
        remote_filepath='/incoming/temp/ohub_2_test/{}'.format(tmp_file.split('/')[-1]),
        ssh_conn_id='acm_sftp_ssh',
        operation=SFTPOperation.PUT
    )

    contactpersons_create_cluster >> contact_persons_file_interface_to_parquet >> contact_persons_exact_match >> begin_fuzzy_matching
    for t in matching_tasks:
        begin_fuzzy_matching >> t >> end_fuzzy_matching
    end_fuzzy_matching >> join_contact_persons >> contact_person_combining >> operators_integrated_sensor >> contact_person_referencing
    contact_person_referencing >> contact_persons_to_acm >> contact_persons_acm_from_wasb >> contact_person_ftp_to_acm
    contact_persons_to_acm >> contactpersons_terminate_cluster
