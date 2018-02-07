# Airflow DAGs

This repository holds the airflow dags for OHUB2.0 and O-Universe.

To start writing a DAG for airflow have a look at the existing DAGs or the [airflow tutorial](https://airflow.apache.org/tutorial.html).

## Deployment

Deploying a new version of airflow DAG automatically is tricky since one would rather not give an arbitrary system access to the airflow machine. As such, often a single DAG in this repository is setup that periodically performs a git pull from this repository; thus updating itself and other DAGs defined here.
