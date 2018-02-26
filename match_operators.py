""" Fuzzy matching of strings using Spark

General Flow:

- Read parquet file
- Pre-process dataframe to form a string column `name` which will
    contain the strings to be matched.
- Get countries with more than 100 entries
- Loop over each of these countries
- Strings are first tokenized using n-grams from the total corpus.
- Tokenized vector is normalized.
- Cosine similarity is calculated by absolute squaring the matrix.
- Collect N number of matches above a threshold
- Group matches and assing a group ID
- Write parquet file partition by country code
"""

import os
import argparse
from typing import List
from time import perf_counter as timer

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

from string_matching.spark_string_matching import match_strings

__author__ = "Rodrigo Agundez"
__version__ = "0.1"
__maintainer__ = "Rodrigo Agundez"
__email__ = "rodrigo.agundez@godatadriven.com"
__status__ = "Development"

EGG_NAME = 'string_matching.egg'
MININUM_OPERATORS_PER_COUNTRY = 100
MATRIX_CHUNK_ROWS = 500
N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
VOCABULARY_SIZE = 1500

LOGGER = None

# characters to be dropped from strings to be compared
# list provided by Roderik von Maltzahn
DROP_CHARS = "\\\\!#%&()*+-/:;<=>?@\\^|~\u00A8\u00A9\u00AA\u00AC\u00AD\u00AF\u00B0" \
             "\u00B1\u00B2\u00B3\u00B6\u00B8\u00B9\u00BA\u00BB\u00BC\u00BD\u00BE" \
             "\u2013\u2014\u2022\u2026\u20AC\u2121\u2122\u2196\u2197\u247F\u250A" \
             "\u2543\u2605\u2606\u3001\u3002\u300C\u300D\u300E\u300F\u3010\u3011" \
             "\uFE36\uFF01\uFF06\uFF08\uFF09\uFF1A\uFF1B\uFF1F{}\u00AE\u00F7\u02F1" \
             "\u02F3\u02F5\u02F6\u02F9\u02FB\u02FC\u02FD\u1BFC\u1BFD\u2260\u2264" \
             "\u2DE2\u2DF2\uEC66\uEC7C\uEC7E\uED2B\uED34\uED3A\uEDAB\uEDFC\uEE3B" \
             "\uEEA3\uEF61\uEFA2\uEFB0\uEFB5\uEFEA\uEFED\uFDAB\uFFB7\u007F\u24D2" \
             "\u2560\u2623\u263A\u2661\u2665\u266A\u2764\uE2B1\uFF0D"
REGEX = "[{}]".format(DROP_CHARS)


def start_spark():
    """
    # assigning at least 4GB to each core
    # NOTE: HDInsight by default allocates a single core per executor
    # regardless of what we set in here. The truth is in Yarn UI not in Spark UI.
    """
    spark = (SparkSession
             .builder
             .appName("NameMatching")
             .config('spark.dynamicAllocation.enabled', False)
             .config('spark.executorEnv.PYTHON_EGG_CACHE', '/tmp')
             .config('spark.executor.instances', 4)
             .config('spark.executor.cores', 13)
             .config('spark.executor.memory', '14g')
             .config('spark.driver.memory', '15g')
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("INFO")

    # the egg file should be in the same path as this script
    dir_path = os.path.dirname(os.path.realpath(__file__))
    sc.addPyFile(os.path.join(dir_path, 'dist', EGG_NAME))

    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().getLogger('org').setLevel(log4j.Level.WARN)
    log4j.LogManager.getRootLogger().getLogger('akka').setLevel(log4j.Level.ERROR)

    global LOGGER
    LOGGER = log4j.LogManager.getLogger('Name Matching')
    return spark


def read_operators(spark: SparkSession, fn: str, fraction: float) -> DataFrame:
    return (spark
            .read
            .parquet(fn)
            .sample(False, fraction))


def preprocess_operators(ddf: DataFrame) -> DataFrame:
    w = Window.partitionBy('COUNTRY_CODE').orderBy(sf.asc('id'))
    return (ddf
            .na.drop(subset=['NAME_CLEANSED'])
            # create unique ID
            .withColumn('id', sf.concat_ws('~',
                                           sf.col('COUNTRY_CODE'),
                                           sf.col('SOURCE'),
                                           sf.col('REF_OPERATOR_ID')))
            .fillna('')
            # create string columns to matched
            .withColumn('name',
                        sf.concat_ws(' ',
                                     sf.col('NAME_CLEANSED'),
                                     sf.col('CITY_CLEANSED'),
                                     sf.col('STREET_CLEANSED'),
                                     sf.col('ZIP_CODE_CLEANSED')))
            .withColumn('name', sf.regexp_replace('name', REGEX, ''))
            .withColumn('name', sf.trim(sf.regexp_replace('name', '\s+', ' ')))
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', 'id', 'name', 'COUNTRY_CODE'))


def get_country_codes(country_code_arg: str, operators: DataFrame) -> List[str]:
    if country_code_arg == 'all':
        opr_count = operators.groupby('COUNTRY_CODE').count()
        LOGGER.info("Selecting countries with more than " + str(MININUM_OPERATORS_PER_COUNTRY) + " entries")
        codes = (
            opr_count[opr_count['count'] > MININUM_OPERATORS_PER_COUNTRY]
            .select('COUNTRY_CODE')
            .distinct()
            .rdd.map(lambda r: r[0]).collect())
    else:
        LOGGER.info("Selecting only country: " + country_code_arg)
        codes = [args.country_code]
    return codes


def select_and_repartition_country(ddf: DataFrame, country_code: str) -> DataFrame:
    return (ddf
            .filter(sf.col('COUNTRY_CODE') == country_code)
            .drop('COUNTRY_CODE')
            .repartition('id')
            .sort('id', ascending=True))


def group_similarities(similarity: DataFrame) -> DataFrame:
    """
    group similarities with column i being the group id
    join name_index with original unique id
    """
    grouping_window = (
        Window
        .partitionBy('j')
        .orderBy(sf.asc('i')))

    # keep only the first entry sorted alphabetically
    grp_sim = (
        similarity
        .withColumn("rn", sf.row_number().over(grouping_window))
        .filter(sf.col("rn") == 1)
        .drop('rn')
    )

    # remove group ID from column j
    return grp_sim.join(
        grp_sim.select('j').subtract(grp_sim.select('i')),
        on='j', how='inner'
    )


def find_matches(grouped_similarity: DataFrame, operators: DataFrame, country_code: str) -> DataFrame:
    return (grouped_similarity
            .join(operators, grouped_similarity['i'] == operators['name_index'],
                  how='left').drop('name_index')
            .selectExpr('i', 'j', 'id as SOURCE_ID',
                        'SIMILARITY', 'name as SOURCE_NAME')
            .join(operators, grouped_similarity['j'] == operators['name_index'],
                  how='left').drop('name_index')
            .withColumn('COUNTRY_CODE', sf.lit(country_code))
            .selectExpr('COUNTRY_CODE', 'SOURCE_ID', 'id as TARGET_ID',
                        'SIMILARITY', 'SOURCE_NAME', 'name as TARGET_NAME'))


def save_to_parquet(matches: DataFrame, fn):
    mode = 'append'
    LOGGER.info("Writing to: " + fn)
    LOGGER.info("Mode: " + mode)

    (matches
        .coalesce(20)
        .write
        .partitionBy('country_code')
        .parquet(fn, mode=mode))


def print_stats(matches: DataFrame):
    matches.persist()
    n_matches = matches.count()

    print('\n\nNr. Similarities:\t', n_matches)
    print('Threshold:\t', args.threshold)
    print('NTop:\t', args.ntop)
    (matches
        .select('SOURCE_ID', 'TARGET_ID',
                'SIMILARITY', 'SOURCE_NAME', 'TARGET_NAME')
        .sort('SIMILARITY', ascending=True)
        .show(50, truncate=False))

    (matches
        .groupBy(['SOURCE_ID', 'SOURCE_NAME'])
        .count()
        .sort('count', ascending=False).show(50, truncate=False))

    matches.describe('SIMILARITY').show()


def name_match_country_operators(spark: SparkSession, country_code: str, all_operators: DataFrame):
    LOGGER.info("Creating row id for country: " + country_code)
    # get country data and add row number column
    operators = select_and_repartition_country(all_operators, country_code)

    LOGGER.info("Calculating similarities")
    similarity = match_strings(
        spark, operators,
        string_column='name',
        row_number_column='name_index',
        n_top=args.ntop,
        threshold=args.threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE,
        matrix_chunks_rows=MATRIX_CHUNK_ROWS
    )

    grouped_similarity = group_similarities(similarity)
    matches = find_matches(grouped_similarity, operators, country_code)

    if args.output_path:
        save_to_parquet(matches, args.output_path)
    else:
        print_stats(matches)


def main(args):
    spark = start_spark()

    t = Timer('Preprocessing operators', LOGGER)
    all_operators = read_operators(spark, args.input_file, args.fraction)
    preprocessed_operators = preprocess_operators(all_operators)

    LOGGER.info("Parsing and persisting operator data")
    preprocessed_operators.persist()
    t.end_and_log()

    country_codes = get_country_codes(args.country_code,
                                      preprocessed_operators)

    for country_code in country_codes:
        t = Timer('Running for country {}'.format(country_code), LOGGER)
        name_match_country_operators(spark,
                                     country_code, preprocessed_operators)
        t.end_and_log()


class Timer(object):
    def __init__(self, name: str, logger):
        self.logger = logger
        self.name = name
        self.start = timer()

    def end_and_log(self):
        end = timer()
        self.logger.info('{} took {} s'.format(self.name, str(end - self.start)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-f', '--input_file',
                        help='fullpath or location of the input parquet file')
    parser.add_argument('-p', '--output_path', default=None,
                        help='write results in a parquet file to this fullpath or location directory')
    parser.add_argument('-c', '--country_code', default='all',
                        help='country code to use (e.g. US). Default all countries.')
    parser.add_argument('-frac', '--fraction', default=1.0, type=float,
                        help='use this fraction of records.')
    parser.add_argument('-t', '--threshold', default=0.75, type=float,
                        help='drop similarities below this value [0-1].')
    parser.add_argument('-n', '--ntop', default=1500, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args)
