# Name Matching

This repo contains the name matching logic for UFS O-Hub operators and contact persons.

The actual matching is based on [sparse_dot_topn](https://github.com/ing-bank/sparse_dot_topn),
a Cython library open-sourced by ING under the Apache 2 license, further elaborated on in a
[Medium post](https://medium.com/wbaa/https-medium-com-ingwbaa-boosting-selection-of-the-most-similar-entities-in-large-scale-datasets-450b3242e618)
and [SlideShare](https://www.slideshare.net/godatadriven/pydata-amsterdam-name-matching-at-scale) by GoDataDriven.

The current repository essentially wraps this library to run on our operator and contact data for initial and delta loads.

On a high level, it vectorises the desired columns into trigram representations,
then uses a [SciPy sparse matrix dot function](https://docs.scipy.org/doc/scipy-0.19.0/reference/generated/scipy.sparse.csr_matrix.dot.html) and a Spark UDF function. Then, the top-n candidates are selected using the NumPy [argpartition](http://%28https//docs.scipy.org/doc/numpy-1.13.0/reference/generated/numpy.argpartition.html) function.

![`sparse_dot_topn` steps](https://cdn-images-1.medium.com/max/800/1*jElpcnRIU_rkByX2nbmweA.png)

# Local development

Make sure you have installed docker

Pull and step into the docker container with

```bash
docker pull fokkodriesprong/docker-pyspark
docker run -it -v $(pwd):/tmp fokkodriesprong/docker-pyspark /bin/bash
```

or in command line:
```cmd
docker pull fokkodriesprong/docker-pyspark
docker run -it -v %CD%:/tmp fokkodriesprong/docker-pyspark /bin/bash
```

Inside the container, `cd` into the working directory and install the python environment
```bash
cd /tmp
conda env create -f environment.yml
source activate name-matching
```

Now you can run all the tests with
```bash
cd string_matching_package
python setup.py pytest --addopts "--cov-config .coveragerc --cov=./ tests"
```
If you get weird path errors (all paths should be relative to the docker container), remove all `__pycache__` folders with
```bash
find  . -name "*pycache*" | exec rm -rf {} \;
```
and rerun the tests. After making changes 


# Usage

Requirements for the cluster and Driver:

- Python 3
- Spark 2
- Pandas
- Scipy
- Numpy

**Compile sparse similarity library**

Create conda env containing the required packages first
```bash
conda env create -f environment.yml
```
Activate the environment
```bash
conda activate name-matching
```

Compile Python library written in `C++` and `Cython`. This will output an `egg`  file.
```bash
./compile_library.sh
```
This script will remove the current `egg` file, compile the code and move the `egg` file to `infra/name-matching	` as `sparse_dot_topn.egg`.


**Submit Spark job application**

Run the spark application script which will:
- group similar data 
- match/link each group to a single master record 
The output is parquet files with columns:
`COUNTRY_CODE`, `SOURCE_ID`, `TARGET_ID`, `SIMILARITY`, `SOURCE_NAME` and `TARGET_NAME`.

```bash
spark-submit --driver-memory 10g --py-files <full_path_to__sparse_dot_topn.egg_file> <full_path_to__match_operators.py_file> -f <location_of_input_parquet_file>
-p <location_of_directory_to_write_output>
```
For a more detailed description of the spark job please refer to the `Fuzzy name-matching algorithm` section.

### Fuzzy name-matching algorithm

In this section I describe the steps performed in the algorithm.

1. Read parquet file
The input parquet file should be partitioned by `COUNTRY_CODE`

2. Create unique `id` column
Concatenate columns `COUNTRY_CODE`, `SOURCE` and `REF_OPERATOR_ID` with the `~` character.

3. Pre-process
Form a string column `name` which will contain the strings to be matched. The following columns are cleaned via a regex and concatenated with an empty space: `NAME_CLEANSED`, `CITY_CLEANSED`, `STREET_CLEANSED` and `ZIP_CODE_CLEANSED`.

4. Row number
Using a window function, ordered on `id `, create a row number column `name_index` for each `COUNTRY_CODE` group.

5. Discard "small" countries
Get a list of countries with more than 100 entries

5. For loop
Process each country from previous list

6. Tokenize strings
Strings are first tokenized using n-grams and TF-IDF.

7. Normalization
Tokenized vector is L2-normalized.

8. Similarity
Cosine similarity is calculated by absolute squaring the resulting TF-IDF matrix.

9. Gather matches
Collect N number of top matches above a similarity threshold.

10. Group matches
Keep only the first match for each entry alphabetically ordered. This will be the `SOURCE_ID`, remove resulting `SOURCE_ID`s from `TARGET_ID` s.

11. Write parquet file partitioned by country code