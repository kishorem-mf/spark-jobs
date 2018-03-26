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

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

from string_matching.spark_string_matching import match_strings
from string_matching import utils


__author__ = "Rodrigo Agundez"
__version__ = "0.2"
__maintainer__ = "Roel Bertens"
__email__ = "rodrigo.agundez@godatadriven.com"
__status__ = "Development"


MATRIX_CHUNK_ROWS = 500
N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
VOCABULARY_SIZE = 1500
LOGGER = None


def preprocess_operators(ddf: DataFrame) -> DataFrame:
    w = Window.partitionBy('countryCode').orderBy(sf.asc('id'))
    return (ddf
            .na.drop(subset=['nameCleansed'])
            # create unique ID
            .withColumn('id', sf.concat_ws('~',
                                           sf.col('countryCode'),
                                           sf.col('source'),
                                           sf.col('refOperatorId')))
            .fillna('')
            # create string columns to matched
            .withColumn('name',
                        sf.concat_ws(' ',
                                     sf.col('nameCleansed'),
                                     sf.col('cityCleansed'),
                                     sf.col('streetCleansed'),
                                     sf.col('zipCodeCleansed')))
            .withColumn('name', sf.regexp_replace('name', utils.REGEX, ''))
            .withColumn('name', sf.trim(sf.regexp_replace('name', '\s+', ' ')))
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', 'id', 'name', 'countryCode'))


def join_original_columns_on_match_ids(grouped_similarity: DataFrame, operators: DataFrame, country_code: str) -> DataFrame:
    return (grouped_similarity
            .join(operators, grouped_similarity['i'] == operators['name_index'],
                  how='left').drop('name_index')
            .selectExpr('i', 'j', 'id as sourceId',
                        'similarity', 'name as sourceName')
            .join(operators, grouped_similarity['j'] == operators['name_index'],
                  how='left').drop('name_index')
            .withColumn('countryCode', sf.lit(country_code))
            .selectExpr('countryCode', 'sourceId', 'id as targetId',
                        'similarity', 'sourceName', 'name as targetName'))


def name_match_country_operators(spark: SparkSession, country_code: str, all_operators: DataFrame,
                                 n_top: int, threshold: float):
    LOGGER.info("Creating row id for country: " + country_code)
    # get country data and add row number column
    operators = utils.select_and_repartition_country(all_operators, country_code)

    LOGGER.info("Calculating similarities")
    similarity = match_strings(
        spark, operators,
        string_column='name',
        row_number_column='name_index',
        n_top=n_top,
        threshold=threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE,
        matrix_chunks_rows=MATRIX_CHUNK_ROWS
    )
    grouped_similarity = utils.group_matches(similarity)
    return join_original_columns_on_match_ids(grouped_similarity, operators, country_code)


def main(args):
    spark = utils.start_spark('Match operators')

    t = utils.Timer('Preprocessing operators', LOGGER)
    all_operators = utils.read_parquet(spark, args.input_file, args.fraction)
    preprocessed_operators = preprocess_operators(all_operators)

    LOGGER.info("Parsing and persisting operator data")
    preprocessed_operators.persist()
    t.end_and_log()

    country_codes = utils.get_country_codes(args.country_code, preprocessed_operators)

    for country_code in country_codes:
        t = utils.Timer('Running for country {}'.format(country_code), LOGGER)
        grouped_matches = name_match_country_operators(spark,
                                                       country_code,
                                                       preprocessed_operators,
                                                       args.n_top,
                                                       args.threshold)
        t.end_and_log()
        if args.output_path:
            utils.save_to_parquet(grouped_matches, args.output_path)
        else:
            utils.print_stats(grouped_matches, args.n_top, args.threshold)
    preprocessed_operators.unpersist()


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
