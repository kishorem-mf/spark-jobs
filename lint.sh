#!/usr/bin/env bash
pycodestyle --show-source .
python -m pytest --cov-config .coveragerc --cov=./dags tests
