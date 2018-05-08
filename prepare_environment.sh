#!/usr/bin/env bash


conda env create -f environment.yml && source activate name-matching

export PYSPARK_PYTHON=/opt/miniconda/envs/name-matching/bin/python
rm -rf dist
./compile_library.sh