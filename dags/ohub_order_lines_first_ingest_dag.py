from airflow import DAG
from ohub_entities_first_ingest_dag import make_first_dag
make_first_dag('order_lines', 'OrderLine')
