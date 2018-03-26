""" Join new operator data with current operator data, keeping persistent group id's

The following steps are performed:
- Load all current operators
- Pre-process current operators to format for joining
- Pre-process new operator data to format for joining
- Per country
    - join all current operators with new operators
    - join the result from matching with all current operators
    - write parquet file
"""

import argparse
import hashlib

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


N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
VOCABULARY_SIZE = 2000


def get_all_current_operators(spark: SparkSession, fn: str) -> DataFrame:
    return (spark
            .read
            .parquet(fn)
            .withColumn('refId', sf.explode('refIds'))
            .drop('refIds')
            .withColumnRenamed('ohubOperatorId', 'ohubOperatorId_old')
            )


def preprocess_current_operators_for_matching(spark: SparkSession, fn: str) -> DataFrame:
    """Create column 'matching_string' used originally for the matching"""
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
            .withColumn('matching_string', sf.regexp_replace('matching_string', utils.REGEX, ''))
            .withColumn('matching_string', sf.lower(sf.trim(sf.regexp_replace(sf.col('matching_string'), '\s+', ' '))))
            .withColumn('string_index', sf.row_number().over(w) - 1)
            .select('countryCode', 'string_index', 'ohubOperatorId', 'matching_string')
            )


def preprocess_new_operators_for_matching(spark: SparkSession, fn: str) -> DataFrame:
    """Create unique 'refID' and 'matching_string' for new input data"""
    w = Window.partitionBy('COUNTRY_CODE').orderBy(sf.asc('refId'))
    return (spark
            .read
            .parquet(fn)
            .na.drop(subset=['NAME_CLEANSED'])
            .withColumn('refId',
                        sf.concat_ws('~',
                                     sf.col('COUNTRY_CODE'),
                                     sf.col('SOURCE'),
                                     sf.col('REF_OPERATOR_ID')
                                     )
                        )
            .fillna('')
            # create string columns to matched
            .withColumn('matching_string',
                        sf.concat_ws(' ',
                                     sf.col('NAME_CLEANSED'),
                                     sf.col('CITY_CLEANSED'),
                                     sf.col('STREET_CLEANSED'),
                                     sf.col('ZIP_CODE_CLEANSED')))
            .withColumn('matching_string', sf.regexp_replace('matching_string', utils.REGEX, ''))
            .withColumn('matching_string', sf.lower(sf.trim(sf.regexp_replace('matching_string', '\s+', ' '))))
            .withColumn('string_index', sf.row_number().over(w) - 1)
            .select('COUNTRY_CODE', 'string_index', 'refId', 'matching_string', 'record_type')
            )


def create_32_char_hash(string):
    hash_id = hashlib.md5(string.encode(encoding='utf-8')).hexdigest()
    return '-'.join([hash_id[:8], hash_id[8:12], hash_id[12:16], hash_id[16:]])


# create udf for use in spark later
udf_create_32_char_hash = sf.udf(create_32_char_hash)


def get_country_codes(new_operators: DataFrame):
    return (new_operators
            .select('COUNTRY_CODE')
            .distinct()
            .rdd.map(lambda r: r[0]).collect()
            )


def join_new_with_all_current_operators(spark, new_operators, all_operators_for_matching, all_operators,
                                        country_code, n_top, threshold):
    new_operators_1country = (
        new_operators
        .filter(sf.col('COUNTRY_CODE') == country_code)
        .repartition('refId')
    )
    all_operators_1country = (
        all_operators_for_matching
        .filter(sf.col('countryCode') == country_code)
        .repartition('ohubOperatorId')
    )
    similarity = match_strings(
        spark,
        new_operators_1country.select('string_index', 'matching_string'),
        df2=all_operators_1country.select('string_index', 'matching_string'),
        string_column='matching_string',
        row_number_column='string_index',
        n_top=n_top,
        threshold=threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE
    )

    # Join on refId and ohubOperatorId to get all columns based on IDs
    matches = (
        new_operators_1country
        .join(similarity, new_operators_1country['string_index'] == similarity['i'], how='left')
        .drop('string_index')
        .selectExpr('j', 'SIMILARITY',
                    'matching_string as matching_string_new', 'refId', 'record_type')
        .join(all_operators_1country, sf.col('j') == all_operators_1country['string_index'], how='left')
        .drop('string_index')
        .withColumn('countryCode', sf.lit(country_code))
        .selectExpr('SIMILARITY',
                    'countryCode',
                    'matching_string_new',
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


def main(arguments):
    spark = utils.start_spark('Join new operators with persistent UUID')
    all_operators = get_all_current_operators(spark, arguments.current_operators_path)
    all_operators_for_matching = preprocess_current_operators_for_matching(spark, arguments.current_operators_path)
    new_operators = preprocess_new_operators_for_matching(spark, arguments.new_operators_path)
    country_codes = get_country_codes(new_operators)

    for country_code in country_codes:
        joined_operators = join_new_with_all_current_operators(spark, new_operators, all_operators_for_matching,
                                                               all_operators, country_code, arguments.n_top, arguments.threshold)
        if arguments.output_path:
            utils.save_to_parquet(joined_operators, arguments.output_path)
        else:
            utils.print_stats(joined_operators, arguments.n_top, arguments.threshold)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-f', '--current_operators_path',
                        help='full path or location of the parquet file with current operators')
    parser.add_argument('-f', '--new_operators_path',
                        help='full path or location of the parquet file with new operators which we want to add')
    parser.add_argument('-p', '--output_path', default=None,
                        help='write results in a parquet file to this full path or location directory')
    parser.add_argument('-c', '--country_code', default='all',
                        help='country code to use (e.g. US). Default all countries.')
    parser.add_argument('-t', '--threshold', default=0.8, type=float,
                        help='drop similarities below this value [0-1].')
    parser.add_argument('-n', '--n_top', default=1, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args)
