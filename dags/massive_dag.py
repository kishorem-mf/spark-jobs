from datetime import datetime

from airflow import DAG

from custom_operators.databricks_functions import \
    DatabricksSubmitRunOperator
from ohub.ohub_dag_config import \
    default_args, databricks_conn_id, jar, ingested_bucket, intermediate_bucket, integrated_bucket, one_day_ago, \
    two_day_ago, postgres_config, \
    GenericPipeline, SubPipeline, DagConfig, large_cluster_config

default_args.update(
    {
        'start_date': datetime(2018, 6, 4)
    }
)
operators_entity = 'operators'
operators_dag_config = DagConfig(
    operators_entity,
    is_delta=True,
    alternate_DAG_entity='massive'
)
contactpersons_entity = 'contactpersons'
contactpersons_dag_config = DagConfig(
    contactpersons_entity,
    is_delta=True,
    alternate_DAG_entity='massive'
)
products_entity = 'products'
products_dag_config = DagConfig(
    products_entity,
    is_delta=True,
    alternate_DAG_entity='massive'
)
orderlines_entity = 'orderlines'
orderlines_dag_config = DagConfig(
    orderlines_entity,
    is_delta=True,
    alternate_DAG_entity='massive'
)
orders_entity = 'orders'
orders_dag_config = DagConfig(
    orders_entity,
    is_delta=True,
    alternate_DAG_entity='massive'
)

with DAG('ohub_massive', default_args=default_args, schedule_interval=operators_dag_config.schedule) as dag:
    operators = (
        GenericPipeline(operators_dag_config, class_prefix='Operator',
                        cluster_config=large_cluster_config(operators_dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='OPERATORS')
            .has_export_to_dispatcher_db(dispatcher_schema_name='OPERATORS')
            .has_ingest_from_file_interface()
            .has_ingest_from_web_event()
    )
    contactpersons = (
        GenericPipeline(contactpersons_dag_config, class_prefix='ContactPerson',
                        cluster_config=large_cluster_config(contactpersons_dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='RECIPIENTS')
            .has_export_to_dispatcher_db(dispatcher_schema_name='CONTACT_PERSONS')
            .has_ingest_from_file_interface()
    )

    orders = (
        GenericPipeline(orders_dag_config, class_prefix='Order',
                        cluster_config=large_cluster_config(orders_dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='ORDERS',
                               extra_acm_parameters=['--orderLineFile',
                                                     integrated_bucket.format(date=one_day_ago, fn='orderlines')])
            .has_export_to_dispatcher_db(dispatcher_schema_name='ORDERS')
            .has_ingest_from_file_interface()
    )

    orderlines = (
        GenericPipeline(orderlines_dag_config, class_prefix='OrderLine',
                        cluster_config=large_cluster_config(orderlines_dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='UFS_ORDERLINES')
            .has_export_to_dispatcher_db(dispatcher_schema_name='ORDERLINES')
            .has_ingest_from_file_interface(deduplicate_on_concat_id=False, alternative_schema='orders')
    )

    products = (
        GenericPipeline(products_dag_config, class_prefix='Product',
                        cluster_config=large_cluster_config(products_dag_config.cluster_name))
            .has_export_to_acm(acm_schema_name='PRODUCTS')
            .has_export_to_dispatcher_db(dispatcher_schema_name='PRODUCTS')
            .has_ingest_from_file_interface()
    )

    # ORDERS / ORDERLINES
    merge_orders = DatabricksSubmitRunOperator(
        task_id=f'{orders_entity}_merge',
        cluster_name=orders_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{Order}Merging",
            'parameters': ['--orderInputFile',
                           ingested_bucket.format(date=one_day_ago, channel='file_interface', fn=orders_entity),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=orders_entity),
                           '--contactPersonInputFile', integrated_bucket.format(date=one_day_ago, fn='contactpersons'),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=orders_entity)]
        })

    merge_orderlines = DatabricksSubmitRunOperator(
        task_id=f'{orderlines_entity}_merge',
        cluster_name=orderlines_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.{OrderLine}Merging",
            'parameters': ['--orderLineInputFile',
                           ingested_bucket.format(date=one_day_ago, channel='file_interface',
                                                  fn=orderlines_dag_config.entity),
                           '--previousIntegrated',
                           integrated_bucket.format(date=two_day_ago, fn=orderlines_dag_config.entity),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=orderlines_dag_config.entity)]
        })

    # OPERATORS

    operators_fuzzy_matching: SubPipeline = operators.construct_fuzzy_matching_pipeline(
        ingest_input=ingested_bucket.format(date=one_day_ago, fn=operators_dag_config.entity, channel='*'),
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        integrated_input=integrated_bucket.format(date=two_day_ago, fn=operators_dag_config.entity),
        delta_match_py='dbfs:/libraries/name_matching/delta_operators.py',
    )

    operators_join = DatabricksSubmitRunOperator(
        task_id=f'operators_merge',
        cluster_name=operators_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta'.format(operators_entity)),
                           '--entityInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_left_overs'.format(operators_entity)),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(operators_entity))] + postgres_config
        })

    operators_combine_to_create_integrated = DatabricksSubmitRunOperator(
        task_id='operators_combine_to_create_integrated',
        cluster_name=operators_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.OperatorCombining",
            'parameters': ['--integratedUpdated',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_fuzzy_matched_delta_integrated'.format(operators_entity)),
                           '--newGolden',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_delta_golden_records'.format(operators_entity)),
                           '--combinedEntities',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(operators_entity))]
        }
    )

    operators_update_golden_records = DatabricksSubmitRunOperator(
        task_id='operators_update_golden_records',
        cluster_name=operators_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.OperatorUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(operators_entity)),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn=operators_entity)] + postgres_config
        }
    )

    # PRODUCTS

    merge_products = DatabricksSubmitRunOperator(
        task_id='products_merge',
        cluster_name=products_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ProductMerging",
            'parameters': ['--productsInputFile',
                           ingested_bucket.format(date=one_day_ago, channel='file_interface', fn=products_entity),
                           '--previousIntegrated', integrated_bucket.format(date=two_day_ago, fn=products_entity),
                           '--outputFile', integrated_bucket.format(date=one_day_ago, fn=products_entity)]
        })

    # CONTACTPERSONS

    contactpersons_pre_processing = DatabricksSubmitRunOperator(
        task_id="contactperson_pre_processed",
        cluster_name=contactpersons_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonPreProcess",
            'parameters': ['--integratedInputFile',
                           integrated_bucket.format(date=two_day_ago, fn=contactpersons_entity),
                           '--deltaInputFile',
                           ingested_bucket.format(date=one_day_ago, fn=contactpersons_entity, channel='*'),
                           '--deltaPreProcessedOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_pre_processed'.format(contactpersons_entity))]
        }
    )

    contactpersons_exact_match_integrated_ingested = DatabricksSubmitRunOperator(
        task_id="contactperson_exact_match_integrated_ingested",
        cluster_name=contactpersons_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonIntegratedExactMatch",
            'parameters': ['--integratedInputFile',
                           integrated_bucket.format(date=two_day_ago, fn=contactpersons_entity),
                           '--deltaInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_pre_processed'.format(contactpersons_entity)),
                           '--matchedExactOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_exact_matches'.format(contactpersons_entity)),
                           '--unmatchedIntegratedOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_integrated'.format(contactpersons_entity)),
                           '--unmatchedDeltaOutputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_delta'.format(contactpersons_entity))
                           ] + postgres_config
        }
    )

    contactpersons_join_fuzzy_matched = DatabricksSubmitRunOperator(
        task_id='contactperson_join_matched',
        cluster_name=contactpersons_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonMatchingJoiner",
            'parameters': ['--matchingInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_fuzzy_matched_delta'.format(contactpersons_entity)),
                           '--entityInputFile', intermediate_bucket.format(date=one_day_ago,
                                                                           fn='{}_delta_left_overs'.format(contactpersons_entity)),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(contactpersons_entity))] + postgres_config
        }
    )

    contactpersons_join_fuzzy_and_exact_matched = DatabricksSubmitRunOperator(
        task_id='contactperson_join_fuzzy_and_exact_matched',
        cluster_name=contactpersons_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.combining.ContactPersonCombineExactAndFuzzyMatches",
            'parameters': ['--contactPersonExactMatchedInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_exact_matches'.format(contactpersons_entity)),
                           '--contactPersonFuzzyMatchedDeltaIntegratedInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_fuzzy_matched_delta_integrated'.format(contactpersons_entity)),
                           '--contactPersonsDeltaGoldenRecordsInputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_delta_golden_records'.format(contactpersons_entity)),
                           '--contactPersonsCombinedOutputFile',
                           intermediate_bucket.format(date=one_day_ago,
                                                      fn='{}_combined'.format(contactpersons_entity))]
        }
    )

    contactpersons_referencing = DatabricksSubmitRunOperator(
        task_id='contactperson_referencing',
        cluster_name=contactpersons_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonReferencing",
            'parameters': ['--combinedInputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_combined'.format(contactpersons_entity)),
                           '--operatorInputFile', integrated_bucket.format(date=one_day_ago, fn='operators'),
                           '--outputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_updated_references'.format(contactpersons_entity))]
        }
    )

    contactpersons_update_golden_records = DatabricksSubmitRunOperator(
        task_id='contactperson_update_golden_records',
        cluster_name=contactpersons_dag_config.cluster_name,
        databricks_conn_id=databricks_conn_id,
        libraries=[
            {'jar': jar}
        ],
        spark_jar_task={
            'main_class_name': "com.unilever.ohub.spark.merging.ContactPersonUpdateGoldenRecord",
            'parameters': ['--inputFile',
                           intermediate_bucket.format(date=one_day_ago, fn='{}_updated_references'.format(contactpersons_entity)),
                           '--outputFile',
                           integrated_bucket.format(date=one_day_ago, fn=contactpersons_entity)] + postgres_config
        }
    )

    ingest_operators: SubPipeline = operators.construct_ingest_pipeline()
    export_operators: SubPipeline = operators.construct_export_pipeline()
    fuzzy_matching_operators: SubPipeline = operators.construct_fuzzy_matching_pipeline(
        ingest_input=ingested_bucket.format(date=one_day_ago, fn=operators_dag_config.entity, channel='*'),
        match_py='dbfs:/libraries/name_matching/match_operators.py',
        integrated_input=integrated_bucket.format(date=two_day_ago, fn=operators_dag_config.entity),
        delta_match_py='dbfs:/libraries/name_matching/delta_operators.py',
    )
    ingest_contactpersons: SubPipeline =  contactpersons.construct_ingest_pipeline()
    export_contactpersons: SubPipeline =  contactpersons.construct_export_pipeline()
    fuzzy_matching_contactpersons: SubPipeline =  contactpersons.construct_fuzzy_matching_pipeline(
        delta_match_py='dbfs:/libraries/name_matching/delta_contacts.py',
        match_py='dbfs:/libraries/name_matching/match_contacts.py',
        integrated_input=intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_integrated'.format(contactpersons_entity)),
        ingest_input=intermediate_bucket.format(date=one_day_ago, fn='{}_unmatched_delta'.format(contactpersons_entity))
    )
    ingest_products: SubPipeline = products.construct_ingest_pipeline()
    export_products: SubPipeline = products.construct_export_pipeline()
    ingest_orders: SubPipeline = orders.construct_ingest_pipeline()
    export_orders: SubPipeline = orders.construct_export_pipeline()
    ingest_orderlines: SubPipeline = orderlines.construct_ingest_pipeline()
    export_orderlines: SubPipeline = orderlines.construct_export_pipeline()

    # OPERATORS
    ingest_operators.last_task >> fuzzy_matching_operators.first_task
    fuzzy_matching_operators.last_task >> operators_join >> operators_combine_to_create_integrated >> operators_update_golden_records
    operators_update_golden_records >> export_operators.first_task

    # PRODUCTS
    ingest_products.last_task >> merge_products >> export_products.first_task

    # ORDERS / ORDERLINES
    ingest_orders.last_task >> ingest_orderlines.first_task
    operators_update_golden_records >> merge_orders
    contactpersons_update_golden_records >> merge_orders
    ingest_orders.last_task >> merge_orders
    merge_orders >> ingest_orderlines.last_task >> export_orders.first_task
    ingest_orderlines.last_task >> merge_orderlines >> export_orderlines.first_task

    # CONTACTPERSONS
    ingest_contactpersons.last_task >> contactpersons_pre_processing >> contactpersons_exact_match_integrated_ingested
    contactpersons_exact_match_integrated_ingested >> fuzzy_matching_contactpersons.first_task
    fuzzy_matching_contactpersons.last_task >> contactpersons_join_fuzzy_matched
    contactpersons_join_fuzzy_matched >> contactpersons_join_fuzzy_and_exact_matched >> contactpersons_referencing
    operators_update_golden_records >> contactpersons_referencing >> contactpersons_update_golden_records >> export_contactpersons.first_task
