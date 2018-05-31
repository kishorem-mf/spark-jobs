from airflow.operators.bash_operator import BashOperator


def construct_operators_pipeline(cluster_name):
    tasks = {}
    tasks['operators_create_cluster'] = BashOperator(
        task_id='operators_create_cluster',
        bash_command='echo "create cluster"'
    )

    tasks['operators_ingest'] = BashOperator(
        task_id='operators_ingest',
        bash_command='echo "ingest"'
    )

    tasks['operators_create_cluster'] >> tasks['operators_ingest']
    return tasks
