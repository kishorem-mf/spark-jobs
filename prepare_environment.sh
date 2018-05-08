#!/usr/bin/env bash

conda env create -f environment.yml
source activate name-matching

cd string_matching_package
python setup.py install