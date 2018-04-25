""" Join ingested daily data with integrated data, keeping persistent group id's

This script outputs two dataframes:
- Updated integrated data
  - Changed records that match with integrated data
  - New records that match with integrated data
- Unmatched data
  - All records that do not match with integrated data

The following steps are performed per country:
- Pre-process integrated data to format for joining
- Pre-process ingested daily data to format for joining
- Match ingested data with integrated data
- write two dataframes to file: updated integrated data and unmatched data
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

from string_matching import utils

N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
MINIMUM_ENTRIES_PER_COUNTRY = 3
VOCABULARY_SIZE = 2000
LOGGER = None


def preprocess_for_matching(ddf: DataFrame, id_column: str, drop_if_name_is_null=False) -> DataFrame:
    """Create column 'matching_string' used originally for the matching"""
    w = Window.partitionBy('countryCode').orderBy(sf.asc(id_column))
    if drop_if_name_is_null:
        ddf = ddf.na.drop(subset=['name'])

    ddf = utils.clean_operator_fields(ddf, 'name', 'city', 'street', 'houseNumber', 'zipCode')

    return (utils.create_operator_matching_string(ddf)
            .filter(sf.col('matching_string') != '')
            .withColumn('string_index', sf.row_number().over(w) - 1)
            .select('countryCode', 'string_index', id_column, 'matching_string')
            )


def join_ingested_daily_with_integrated_operators(spark, ingested_daily, integrated,
                                                  country_code, n_top, threshold):
    from string_matching.spark_string_matching import match_strings
    ingested_daily_1country = (ingested_daily
                               .filter(sf.col('countryCode') == country_code)
                               .repartition('concatId')
                               )
    integrated_1country = (integrated
                           .filter(sf.col('countryCode') == country_code)
                           .repartition('ohubId')
                           )

    similarity = match_strings(
        spark,
        ingested_daily_1country.select('string_index', 'matching_string'),
        df2=integrated_1country.select('string_index', 'matching_string'),
        string_column='matching_string',
        row_number_column='string_index',
        n_top=n_top,
        threshold=threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE
    )

    window = Window.partitionBy('i').orderBy(sf.desc('SIMILARITY'), 'j')
    best_match = (similarity
                  .withColumn('j', sf.first('j').over(window))
                  .drop_duplicates()
                  )

    # Join on string_index to get back the concatId and ohubId
    matched_ingested_daily = (
        best_match
        .join(ingested_daily_1country, ingested_daily_1country['string_index'] == best_match['i'])
        .drop('string_index')
        .selectExpr('j', 'SIMILARITY',
                    'matching_string as matching_string_new', 'concatId')
        .join(integrated_1country, sf.col('j') == integrated_1country['string_index'])
        .drop('string_index')
        .withColumn('countryCode', sf.lit(country_code))
        .selectExpr('SIMILARITY',
                    'countryCode',
                    'matching_string_new',
                    'matching_string as matching_string_old',
                    'concatId',
                    'ohubId as ohubId_matched')
    )
    return matched_ingested_daily


def main(arguments):
    global LOGGER
    spark, LOGGER = utils.start_spark('Match and join newly ingested operators with persistent ohubId')

    ingested_daily = spark.read.parquet(arguments.ingested_daily_operators_input_path)
    ingested_daily_for_matching = preprocess_for_matching(ingested_daily, 'concatId', True)

    integrated = spark.read.parquet(arguments.integrated_operators_input_path)
    integrated_for_matching = preprocess_for_matching(integrated, 'ohubId')

    country_codes_ingested = utils.get_country_codes(arguments.country_code, ingested_daily_for_matching,
                                                     MINIMUM_ENTRIES_PER_COUNTRY)
    country_codes_integrated = utils.get_country_codes(arguments.country_code, integrated_for_matching)
    country_codes = list(set(country_codes_ingested) & set(country_codes_integrated))

    if len(country_codes) == 1:
        save_fun = utils.save_to_parquet_per_partition('countryCode', country_codes[0])
    else:
        save_fun = utils.save_to_parquet

    mode = 'overwrite'
    for i, country_code in enumerate(country_codes):
        if i >= 1:
            mode = 'append'

        LOGGER.info('Match the ingested data with the integrated data for country {}'.format(country_code))
        matched_ingested_daily = join_ingested_daily_with_integrated_operators(
            spark, ingested_daily_for_matching, integrated_for_matching,
            country_code, arguments.n_top, arguments.threshold)

        LOGGER.info('Select matched records from ingested daily and set their ohubId')
        matched_ingested_daily_full_record = (matched_ingested_daily
                                              .select('concatId', 'ohubId_matched')
                                              .join(ingested_daily.filter(sf.col('countryCode') == country_code),
                                                    on='concatId', how='left')
                                              .withColumn('ohubId', sf.col('ohubId_matched'))
                                              .drop('ohubId_matched')
                                              )
        LOGGER.info('Merge the integrated data with the matched ingested daily data')
        updated_integrated = (integrated.filter(sf.col('countryCode') == country_code)
                              .join(matched_ingested_daily_full_record, on='concatId', how='left_anti')
                              .union(matched_ingested_daily_full_record)
                              )

        LOGGER.info('Select the unmatched records from ingested daily')
        unmatched = (ingested_daily.filter(sf.col('countryCode') == country_code)
                     .join(matched_ingested_daily, on='concatId', how='left_anti')
                     )

        LOGGER.info('Write to parquet for country {}'.format(country_code))
        if arguments.updated_integrated_output_path is not None:
            save_fun(updated_integrated, arguments.updated_integrated_output_path, mode)
        if arguments.unmatched_output_path is not None:
            save_fun(unmatched, arguments.unmatched_output_path, mode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('-f', '--integrated_operators_input_path',
                        help='full path or location of the parquet file with integrated operator data')
    parser.add_argument('-g', '--ingested_daily_operators_input_path',
                        help='full path or location of the parquet file with ingested daily operator data')

    parser.add_argument('-p', '--updated_integrated_output_path', default=None,
                        help='write results in a parquet file to this full path or location directory')
    parser.add_argument('-q', '--unmatched_output_path', default=None,
                        help='write results in a parquet file to this full path or location directory')

    parser.add_argument('-c', '--country_code', default='all',
                        help='country code to use (e.g. US). Default all countries.')
    parser.add_argument('-t', '--threshold', default=0.8, type=float,
                        help='drop similarities below this value [0.-1.].')
    parser.add_argument('-n', '--n_top', default=1500, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args)
