from airflow.operators.bash_operator import BashOperator


def construct_contact_persons_pipeline(cluster_name):
    tasks = {}
    tasks['contact_persons_create_cluster'] = BashOperator(
        task_id='contact_persons_create_cluster',
        bash_command='echo "create cluster"'
    )

    tasks['contact_persons_ingest'] = BashOperator(
        task_id='contact_persons_ingest',
        bash_command='echo "ingest"'
    )

    tasks['contact_persons_update_references'] = BashOperator(
        task_id='contact_persons_update_references',
        bash_command='echo "update_references"'
    )

    tasks['contact_persons_create_cluster'] >> tasks['contact_persons_ingest'] >> tasks['contact_persons_update_references']
    return tasks
