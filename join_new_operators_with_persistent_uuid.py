""" Matching of contact persons based on name and location.

Only match contacts without e-mail AND without mobile phone number,
because contacts are already matched on this info.

The following steps are performed:
- keep only contacts without e-mail AND without mobile phone number
- remove contacts without first AND without last name (cleansed)
- remove contacts without a street (cleansed)
- create a unique ID as COUNTRY_CODE~SOURCE~REF_CONTACT_PERSON_ID
- per country
    - match on concatenation of first name and last name
    - keep only the matches with similarity above threshold (0.7)
    - keep only the matches with exactly matching zip code
        - if no zip code is present: keep match if cities (cleansed) matches exactly
    - keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    - to generate a final list of matches, in the form of (i, j), i.e. contact i matches with contact j,
      we do the following:
        - make sure each j only matches with one i (the 'group leader')
            - note: of course we can have multiple matches per group leader, e.g. (i, j) and (i, k)
        - make sure that each i (group leader) is not matched with another 'group leader',
        e.g. if we have (i, j) we remove all (k, i)
- write parquet file partitioned by country code
"""

import argparse
import os
import sys

from typing import List
from time import perf_counter as timer

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window


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
VOCABULARY_SIZE = 1500
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
             .appName("MatchingContacts")
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
    LOGGER = log4j.LogManager.getLogger('Matching Contacts')
    return spark


def read_contacts(spark: SparkSession, fn: str, fraction: float) -> DataFrame:
    return (
        spark
        .read
        .parquet(fn)
        .sample(False, fraction)
    )


def preprocess_contacts(ddf: DataFrame) -> DataFrame:
    w = Window.partitionBy('countryCode').orderBy(sf.asc('id'))
    return (
        ddf
        # keep only if no email nor phone
        .filter(sf.isnull(sf.col('EMAIL_ADDRESS')) & sf.isnull(sf.col('MOBILE_PHONE_NUMBER')))
        # drop if no first name and no last name
        .na.drop(subset=['FIRST_NAME_CLEANSED', 'LAST_NAME_CLEANSED'], how='all')
        # drop if no street
        .na.drop(subset=['STREET_CLEANSED'], how='any')
        # same logic but for an empty string
        .filter((sf.trim(sf.col('STREET_CLEANSED')) != '') &
                ((sf.trim(sf.col('FIRST_NAME_CLEANSED')) != '') | (sf.trim(sf.col('LAST_NAME_CLEANSED')) != '')))
        # create unique ID
        .withColumn('id', sf.concat_ws('~',
                                       sf.col('COUNTRY_CODE'),
                                       sf.col('SOURCE'),
                                       sf.col('REF_CONTACT_PERSON_ID')))
        .fillna('')
        # create string columns to matched
        .withColumn('name',
                    sf.concat_ws(' ',
                                 sf.col('FIRST_NAME_CLEANSED'),
                                 sf.col('LAST_NAME_CLEANSED')))
        .withColumn('name', sf.regexp_replace('name', REGEX, ''))
        .withColumn('name', sf.trim(sf.regexp_replace('name', '\s+', ' ')))
        .withColumn('name_index', sf.row_number().over(w) - 1)
        .select('name_index', 'id', 'name', 'COUNTRY_CODE', 'FIRST_NAME_CLEANSED', 'LAST_NAME_CLEANSED',
                'STREET_CLEANSED', 'HOUSENUMBER', 'ZIP_CODE_CLEANSED', 'CITY_CLEANSED')
    )


def get_country_codes(country_code_arg: str, ddf: DataFrame) -> List[str]:
    if country_code_arg == 'all':
        count_per_country = ddf.groupby('countryCode').count()
        LOGGER.info("Selecting countries with more than " + str(MININUM_CONTACTS_PER_COUNTRY) + " entries")
        codes = (
            count_per_country[count_per_country['count'] > MININUM_CONTACTS_PER_COUNTRY]
            .select('countryCode')
            .distinct()
            .rdd.map(lambda r: r[0]).collect())
    else:
        LOGGER.info("Selecting only country: " + country_code_arg)
        codes = [country_code_arg]
    return codes


def select_and_repartition_country(preprocessed_contacts: DataFrame, country_code: str) -> DataFrame:
    return (
        preprocessed_contacts
        .filter(sf.col('countryCode') == country_code)
        .drop('countryCode')
        .repartition('id')
        .sort('id', ascending=True)
    )


def find_matches(similarity: DataFrame, contacts: DataFrame, country_code: str) -> DataFrame:
    return (
        similarity
        .join(contacts, similarity['i'] == contacts['name_index'],
              how='left').drop('name_index')
        .selectExpr('i', 'j', 'id as SOURCE_ID',
                    'SIMILARITY', 'name as SOURCE_NAME',
                    'STREET_CLEANSED as SOURCE_STREET',
                    'ZIP_CODE_CLEANSED as SOURCE_ZIP_CODE',
                    'CITY_CLEANSED as SOURCE_CITY')
        .join(contacts, similarity['j'] == contacts['name_index'],
              how='left').drop('name_index')
        .withColumn('COUNTRY_CODE', sf.lit(country_code))
        .selectExpr('i', 'j', 'COUNTRY_CODE', 'SOURCE_ID',
                    'id as TARGET_ID', 'SIMILARITY',
                    'SOURCE_NAME', 'STREET_CLEANSED as TARGET_STREET',
                    'SOURCE_STREET', 'name as TARGET_NAME',
                    'SOURCE_ZIP_CODE', 'ZIP_CODE_CLEANSED as TARGET_ZIP_CODE',
                    'SOURCE_CITY', 'CITY_CLEANSED as TARGET_CITY')
        .filter(
            (sf.col('SOURCE_ZIP_CODE') == sf.col('TARGET_ZIP_CODE')) |
            (
                sf.isnull('SOURCE_ZIP_CODE') &
                sf.isnull('TARGET_ZIP_CODE') &
                (sf.col('SOURCE_CITY') == sf.col('TARGET_CITY'))
            )
        )
        .withColumn('street_lev_distance', sf.levenshtein(sf.col('SOURCE_STREET'), sf.col('TARGET_STREET')))
        .filter(sf.col('street_lev_distance') < MIN_LEVENSHTEIN_DISTANCE)
    )


def group_matches(matches: DataFrame) -> DataFrame:
    """ group matches with column i being the group id """
    grouping_window = (
        Window
        .partitionBy('j')
        .orderBy(sf.asc('i')))

    # keep only the first entry sorted alphabetically
    grp_sim = (
        matches
        .withColumn("rn", sf.row_number().over(grouping_window))
        .filter(sf.col("rn") == 1)
        .drop('rn')
    )

    # remove group ID from column j
    return grp_sim.join(
        grp_sim.select('j').subtract(grp_sim.select('i')),
        on='j', how='inner'
    )


def save_to_parquet(matches: DataFrame, fn):
    mode = 'append'
    LOGGER.info("Writing to: " + fn)
    LOGGER.info("Mode: " + mode)

    (matches
        .coalesce(20)
        .write
        .partitionBy('countryCode')
        .parquet(fn, mode=mode))


def print_stats(matches: DataFrame, n_top, threshold):
    matches.persist()
    n_matches = matches.count()

    print('\n\nNr. Similarities:\t', n_matches)
    print('Threshold:\t', threshold)
    print('N_top:\t', n_top)
    (matches
        .select('sourceId', 'targetId',
                'similarity', 'sourceName', 'targetName')
        .sort('similarity', ascending=True)
        .show(50, truncate=False))

    (matches
        .groupBy(['sourceId', 'sourceName'])
        .count()
        .sort('count', ascending=False).show(50, truncate=False))

    matches.describe('similarity').show()


def name_match_country_contacts(spark: SparkSession, country_code: str, preprocessed_contacts: DataFrame,
                                n_top, threshold):
    LOGGER.info("Creating row id for country: " + country_code)
    # get country data and add row number column
    contacts = select_and_repartition_country(preprocessed_contacts, country_code)

    LOGGER.info("Calculating similarities")

    from string_matching.spark_string_matching import match_strings
    similarity = match_strings(
        spark, contacts,
        string_column='name',
        row_number_column='name_index',
        n_top=n_top,
        threshold=threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE,
        matrix_chunks_rows=MATRIX_CHUNK_ROWS
    )

    matches = find_matches(similarity, contacts, country_code)
    return group_matches(matches)


def main(args):
    spark = start_spark()

    t = Timer('Preprocessing contacts', LOGGER)
    all_contacts= read_contacts(spark, args.input_file, args.fraction)
    preprocessed_contacts = preprocess_contacts(all_contacts)

    LOGGER.info("Parsing and persisting contacts data")
    preprocessed_contacts.persist()
    t.end_and_log()

    country_codes = get_country_codes(args.country_code,
                                      preprocessed_contacts)

    for country_code in country_codes:
        t = Timer('Running for country {}'.format(country_code), LOGGER)
        grouped_matches = name_match_country_contacts(spark, country_code, preprocessed_contacts,
                                                      args.n_top, args.threshold)
        t.end_and_log()

        if args.output_path:
            save_to_parquet(grouped_matches, args.output_path)
        else:
            print_stats(grouped_matches, args.n_top, args.threshold)



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
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args)
