""" Matching of contact persons based on name and location.

Only match contacts without e-mail AND without mobile phone number,
because contacts are already matched on this info.

The following steps are performed:
- keep only contacts without e-mail AND without mobile phone number
- remove contacts without first AND without last name (cleansed)
- remove contacts without a street (cleansed)
- create a unique ID as COUNTRY_CODE~SOURCE~REF_CONTACT_PERSON_ID
- create a matching-string: concatenation of first name and last name
- per country
    - match on matching-string
    - keep only the matches with similarity above threshold (0.7)
    - keep only the matches with exactly matching zip code
        - if no zip code is present: keep match if cities (cleansed) match exactly
    - keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    - to generate a final list of matches, in the form of (i, j), i.e. contact i matches with contact j,
      we do the following:
        - make sure each j only matches with one i (the 'group leader')
            - note: of course we can have multiple matches per group leader, e.g. (i, j) and (i, k)
        - make sure that each i (group leader) is not matched with another 'group leader',
        e.g. if we have (i, j) we remove (k, i) for all k
- write parquet file partitioned by country code
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

import utils
from string_matching.spark_string_matching import match_strings


__author__ = "Roel Bertens"
__version__ = "0.1"
__maintainer__ = "Roel Bertens"
__email__ = "roelbertens@godatadriven.com"
__status__ = "Development"


MATRIX_CHUNK_ROWS = 500
N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
VOCABULARY_SIZE = 1500
MIN_LEVENSHTEIN_DISTANCE = 5
LOGGER = None


def preprocess_contacts(ddf: DataFrame) -> DataFrame:
    """Some pre-processing
        - keep only contacts without e-mail AND without mobile phone number
        - remove contacts without first AND without last name (cleansed)
        - remove contacts without a street (cleansed)
        - create a unique ID
        - create matching-string
        - select only necessary columns
    """
    w = Window.partitionBy('COUNTRY_CODE').orderBy(sf.asc('id'))
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
        .withColumn('name', sf.regexp_replace('name', utils.REGEX, ''))
        .withColumn('name', sf.trim(sf.regexp_replace('name', '\s+', ' ')))
        .withColumn('name_index', sf.row_number().over(w) - 1)
        .select('name_index', 'id', 'name', 'COUNTRY_CODE', 'FIRST_NAME_CLEANSED', 'LAST_NAME_CLEANSED',
                'STREET_CLEANSED', 'HOUSENUMBER', 'ZIP_CODE_CLEANSED', 'CITY_CLEANSED')
    )


def join_columns_and_filter(similarity: DataFrame, contacts: DataFrame, country_code: str) -> DataFrame:
    """Join back the original columns (street, zip, etc.) after matching and filter matches as follows:
        - keep only the matches with exactly matching zip code
            - if no zip code is present: keep match if cities (cleansed) match exactly
        - keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    """
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


def match_contacts_for_country(spark: SparkSession, country_code: str, preprocessed_contacts: DataFrame,
                               n_top: int, threshold: float):
    """Match contacts for a single country"""
    LOGGER.info("Matching contacts for country: " + country_code)
    contacts = utils.select_and_repartition_country(preprocessed_contacts, country_code)
    LOGGER.info("Calculating similarities")
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
    LOGGER.info("Group matches")
    grouped_similarity = utils.group_matches(similarity)
    LOGGER.info("Join matches with original columns, filter, and return result")
    return join_columns_and_filter(grouped_similarity, contacts, country_code)


def main(arguments):
    spark = utils.start_spark('Match contacts')

    t = utils.Timer('Preprocessing contacts', LOGGER)
    all_contacts = utils.read_parquet(spark, arguments.input_file, arguments.fraction)
    preprocessed_contacts = preprocess_contacts(all_contacts)
    LOGGER.info("Parsing and persisting contacts data")
    preprocessed_contacts.persist()
    t.end_and_log()

    country_codes = utils.get_country_codes(arguments.country_code, preprocessed_contacts)
    for country_code in country_codes:
        t = utils.Timer('Running for country {}'.format(country_code), LOGGER)
        grouped_matches = match_contacts_for_country(spark,
                                                     country_code,
                                                     preprocessed_contacts,
                                                     arguments.n_top,
                                                     arguments.threshold)
        t.end_and_log()
        if arguments.output_path:
            utils.save_to_parquet(grouped_matches, arguments.output_path)
        else:
            utils.print_stats(grouped_matches, arguments.n_top, arguments.threshold)
    preprocessed_contacts.unpersist()


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
