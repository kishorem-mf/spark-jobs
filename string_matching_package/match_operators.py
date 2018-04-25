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

from string_matching import utils

MATRIX_CHUNK_ROWS = 500
N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
VOCABULARY_SIZE = 1500
LOGGER = None


def preprocess_operators(ddf: DataFrame) -> DataFrame:
    """Create a unique ID and the string that is used for matching and select only necessary columns"""
    w = Window.partitionBy('countryCode').orderBy(sf.asc('id'))
    ddf = utils.clean_operator_fields(ddf, 'name', 'city', 'street', 'houseNumber', 'zipCode')

    ddf = (ddf.na.drop(subset=['nameCleansed'])
           .withColumnRenamed('concatId', 'id')
           .fillna(''))

    return (utils.create_operator_matching_string(ddf)
            .filter(sf.col('matching_string') != '')
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', 'id', 'matching_string', 'countryCode')
            .withColumnRenamed('matching_string', 'name'))


def join_original_columns(grouped_similarity: DataFrame, operators: DataFrame, country_code: str) -> DataFrame:
    """Join back the original name and ID columns after matching"""
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


def match_operators_for_country(spark: SparkSession, country_code: str, all_operators: DataFrame,
                                n_top: int, threshold: float):
    from string_matching.spark_string_matching import match_strings
    """Match operators for a single country"""
    LOGGER.info("Matching operators for country: " + country_code)
    operators = utils.select_and_repartition_country(all_operators, 'countryCode', country_code)
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
    LOGGER.info("Group matches")
    grouped_similarity = utils.group_matches(similarity)
    LOGGER.info("Join matches with original columns and return result")
    return join_original_columns(grouped_similarity, operators, country_code)


def main(arguments):
    global LOGGER
    spark, LOGGER = utils.start_spark('Match operators')

    t = utils.Timer('Preprocessing operators', LOGGER)
    all_operators = utils.read_parquet(spark, arguments.input_file, arguments.fraction)
    preprocessed_operators = preprocess_operators(all_operators)
    LOGGER.info("Parsing and persisting operator data")
    preprocessed_operators.persist()
    t.end_and_log()

    country_codes = utils.get_country_codes(arguments.country_code, preprocessed_operators)

    if len(country_codes) == 1:
        save_fun = utils.save_to_parquet_per_partition('countryCode', country_codes[0])
    else:
        save_fun = utils.save_to_parquet

    mode = 'overwrite'
    for i, country_code in enumerate(country_codes):
        if i >= 1:
            mode = 'append'
        t = utils.Timer('Running for country {}'.format(country_code), LOGGER)
        grouped_matches = match_operators_for_country(spark,
                                                      country_code,
                                                      preprocessed_operators,
                                                      arguments.n_top,
                                                      arguments.threshold)
        t.end_and_log()
        if arguments.output_path:
            save_fun(grouped_matches, arguments.output_path, mode)
        else:
            utils.print_stats_operators(grouped_matches, arguments.n_top, arguments.threshold)
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
    parser.add_argument('-t', '--threshold', default=0.8, type=float,
                        help='drop similarities below this value [0-1].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args)
