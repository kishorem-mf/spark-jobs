#!/usr/bin/env bash
conda env create -f environment.yml
source activate airflow-dags
pycodestyle --show-source .
python -m pytest --cov-config .coveragerc --cov=./dags tests
