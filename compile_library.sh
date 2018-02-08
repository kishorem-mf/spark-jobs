#!/bin/bash
set -e

# Compile c++ library using the Cython wrapper

# move to root folder
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/cython
cd $DIR

rm -rf dist
rm -rf build

# compile the library and build the egg file containing the module
python setup.py bdist_egg

# copy to the main folder
cp dist/*.egg ../sparse_dot_topn.egg

