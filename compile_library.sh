#!/bin/bash
set -e

# Compile c++ library using the Cython wrapper

# move to root folder
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/string_matching_package"
cd $DIR

rm -rf dist
rm -rf build

# compile the library and build the egg file containing the module
python setup.py bdist_egg


# rename to persistent name
mkdir ../dist
cp dist/*.egg ../dist/string_matching.egg
