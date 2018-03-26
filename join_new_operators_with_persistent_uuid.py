""" Join new operator data with current operator data, keeping persisten group id's

The following steps are performed:
-
- write parquet file ....
"""

import argparse
import os
import sys
import hashlib

from typing import List
from time import perf_counter as timer

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

from string_matching.spark_string_matching import match_strings


__author__ = "Roel Bertens"
__version__ = "0.1"
__maintainer__ = "Roel Bertens"
__email__ = "roelbertens@godatadriven.com"
__status__ = "Development"

EGG_NAME = 'string_matching.egg'
MININUM_CONTACTS_PER_COUNTRY = 100
MATRIX_CHUNK_ROWS = 500
N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
VOCABULARY_SIZE = 2000
MIN_LEVENSHTEIN_DISTANCE = 5

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
             .appName("JoinOperatorsPersistentUUID")
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
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    # sys.path.append(os.path.join(dir_path, 'dist', EGG_NAME))
    # sc.addPyFile(os.path.join(dir_path, 'dist', EGG_NAME))

    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().getLogger('org').setLevel(log4j.Level.WARN)
    log4j.LogManager.getRootLogger().getLogger('akka').setLevel(log4j.Level.ERROR)

    global LOGGER
    LOGGER = log4j.LogManager.getLogger('Join operators with persistent UUID')
    return spark


def get_all_operators(spark: SparkSession, fn: str) -> DataFrame:
    return (spark
            .read
            .parquet(fn)
            .withColumn('refId', sf.explode('refIds'))
            .drop('refIds')
            .withColumnRenamed('ohubOperatorId', 'ohubOperatorId_old')
            )


def get_old_operators_for_matching(spark: SparkSession, fn: str) -> DataFrame:
    # Create string name used originally for the matching
    w = Window.partitionBy('countryCode').orderBy(sf.asc('ohubOperatorId'))

    return (spark
            .read
            .parquet(fn)
            .withColumn('matching_string',
                        sf.concat_ws(' ',
                                     sf.col('operator.nameCleansed'),
                                     sf.col('operator.cityCleansed'),
                                     sf.col('operator.streetCleansed'),
                                     sf.col('operator.zipCodeCleansed')
                                     )
                        )
            .withColumn('matching_string', sf.regexp_replace('matching_string', REGEX, ''))
            .withColumn('matching_string', sf.lower(sf.trim(sf.regexp_replace(sf.col('matching_string'), '\s+', ' '))))
            .withColumn('string_index', sf.row_number().over(w) - 1)
            .select('countryCode', 'string_index', 'ohubOperatorId', 'matching_string')
            )


def preprocess_new_operators(spark: SparkSession, fn: str) -> DataFrame:
    w = Window.partitionBy('COUNTRY_CODE').orderBy(sf.asc('refId'))
    return (spark
            .read
            .parquet(fn)
            .na.drop(subset=['NAME_CLEANSED'])
            .withColumn('refId', sf.concat_ws('~',
                                   sf.col('COUNTRY_CODE'),
                                   sf.col('SOURCE'),
                                   sf.col('REF_OPERATOR_ID')))
            .fillna('')
            # create string columns to matched
            .withColumn('matching_string',
                        sf.concat_ws(' ',
                                     sf.col('NAME_CLEANSED'),
                                     sf.col('CITY_CLEANSED'),
                                     sf.col('STREET_CLEANSED'),
                                     sf.col('ZIP_CODE_CLEANSED')))
            .withColumn('matching_string', sf.regexp_replace('matching_string', REGEX, ''))
            .withColumn('matching_string', sf.lower(sf.trim(sf.regexp_replace('matching_string', '\s+', ' '))))
            .withColumn('string_index', sf.row_number().over(w) - 1)
            .select('COUNTRY_CODE', 'string_index', 'refId', 'matching_string', 'record_type')
            )


def create_32_char_hash(string):
    hash_id = hashlib.md5(string.encode(encoding='utf-8')).hexdigest()
    return '-'.join([hash_id[:8], hash_id[8:12], hash_id[12:16], hash_id[16:]])

# create udf for use in spark later
udf_create_32_char_hash = sf.udf(create_32_char_hash)


class Timer(object):
    def __init__(self, name: str, logger):
        self.logger = logger
        self.name = name
        self.start = timer()

    def end_and_log(self):
        end = timer()
        self.logger.info('{} took {} s'.format(self.name, str(end - self.start)))


def get_country_codes(new_operators: DataFrame):
    return (new_operators
            .select('COUNTRY_CODE')
            .distinct()
            .rdd.map(lambda r: r[0]).collect()
            )

def magic_per_country(spark, new_operators, old_operators_for_matching, all_operators, country_code, n_top, threshold):
    input_oprs_ctr = new_operators.filter(sf.col('COUNTRY_CODE') == country_code).repartition('refId')
    oprs_old_ctr = old_operators_for_matching.filter(sf.col('countryCode') == country_code).repartition('ohubOperatorId')

    similarity = match_strings(
        spark,
        input_oprs_ctr.select('string_index', 'matching_string'),
        df2=oprs_old_ctr.select('string_index', 'matching_string'),
        string_column='matching_string',
        row_number_column='string_index',
        n_top=n_top,
        threshold=threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE
    )

    # Join to original refId and ohubOperatorId
    matches = (
        input_oprs_ctr
            .join(similarity, input_oprs_ctr['string_index'] == similarity['i'], how='left')
            .drop('string_index')
            .selectExpr('j', 'SIMILARITY',
                        'matching_string as matching_string_input', 'refId', 'record_type')
            .join(oprs_old_ctr, sf.col('j') == oprs_old_ctr['string_index'], how='left')
            .drop('string_index')
            .withColumn('countryCode', sf.lit(country_code))
            .selectExpr('SIMILARITY',
                        'countryCode',
                        'matching_string_input',
                        'matching_string as matching_string_old',
                        'refId',
                        'ohubOperatorId as ohubOperatorId_matched',
                        'record_type')
    )

    # Updating UUID
    return (matches
            .join(all_operators.filter(sf.col('countryCode') == country_code),
                  on=['refId', 'countryCode'], how='outer')
            .withColumn('ohubOperatorId_new',
                        sf.when(sf.col('ohubOperatorId_matched').isNotNull(),
                                sf.col('ohubOperatorId_matched'))
                        .when(sf.col('ohubOperatorId_old').isNotNull(),
                              sf.col('ohubOperatorId_old'))
                        .otherwise(udf_create_32_char_hash(sf.col('refId')))
                        )
            )


def save_to_parquet(joined_operators: DataFrame, fn):
    mode = 'append'
    LOGGER.info("Writing to: " + fn)
    LOGGER.info("Mode: " + mode)
    (
        joined_operators
        .coalesce(20)
        .write
        .partitionBy('countryCode')
        .parquet(fn, mode=mode)
    )


def main(args):
    spark = start_spark()
    all_operators = get_all_operators(spark, args.input_path)
    old_operators_for_matching = get_old_operators_for_matching(spark, args.input_path)
    new_operators = preprocess_new_operators(spark, args.output_path)
    country_codes = get_country_codes()

    for country_code in country_codes:
        joined_operators = magic_per_country(spark, new_operators, old_operators_for_matching, all_operators,
                                             country_code, args.n_top, args.threshold)
        if args.output_path:
            save_to_parquet(joined_operators, args.output_path)
        else:
            print_stats(joined_operators, args.n_top, args.threshold)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-f', '--input_path',
                        help='full path or location of the parquet file with current operators')
    parser.add_argument('-p', '--output_path', default=None,
                        help='write results in a parquet file to this full path or location directory')
    parser.add_argument('-c', '--country_code', default='all',
                        help='country code to use (e.g. US). Default all countries.')
    parser.add_argument('-t', '--threshold', default=0.75, type=float,
                        help='drop similarities below this value [0-1].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args)
