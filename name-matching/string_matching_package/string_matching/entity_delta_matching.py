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
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

from .entity_matching import \
    N_GRAMS, MINIMUM_DOCUMENT_FREQUENCY, VOCABULARY_SIZE, MINIMUM_ENTRIES_PER_COUNTRY, MIN_LEVENSHTEIN_DISTANCE
from .utils import start_spark, Timer, save_to_parquet_per_partition
from .spark_string_matching import similarity_schema


def match_delta_entity_for_country(spark, ingested_daily, integrated, n_top, threshold):
    from .spark_string_matching import match_strings

    if not (ingested_daily.count() >= MINIMUM_ENTRIES_PER_COUNTRY and
            integrated.count() >= MINIMUM_ENTRIES_PER_COUNTRY):
        return spark.createDataFrame([], similarity_schema)

    similarity = match_strings(
        spark,
        df=ingested_daily.select('name_index', 'matching_string'),
        df2=integrated.select('name_index', 'matching_string'),
        string_column='matching_string',
        row_number_column='name_index',
        n_top=n_top,
        threshold=threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE
    )

    return similarity


def postprocess_delta_contact_persons(similarity: DataFrame,
                                      ingested_preprocessed: DataFrame,
                                      integrated_preprocessed: DataFrame):
    """Join back the original columns (street, zip, etc.) after matching and filter matches as follows:
        - keep only the matches with exactly matching zip code
            - if no zip code is present: keep match if cities (cleansed) match exactly
        - keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    """
    similarity_filtered = (similarity
                           .join(ingested_preprocessed, similarity['i'] == ingested_preprocessed['name_index'])
                           .selectExpr('j',
                                       'concatId',
                                       'SIMILARITY',
                                       'streetCleansed as sourceStreet',
                                       'zipCodeCleansed as sourceZipCode',
                                       'cityCleansed as sourceCity')
                           .join(integrated_preprocessed, similarity['j'] == integrated_preprocessed['name_index'])
                           .selectExpr('concatId',
                                       'ohubId',
                                       'SIMILARITY',
                                       'sourceStreet', 'streetCleansed as targetStreet',
                                       'sourceZipCode', 'zipCodeCleansed as targetZipCode',
                                       'sourceCity', 'cityCleansed as targetCity')
                           .filter((sf.col('sourceZipCode') == sf.col('targetZipCode')) |
                                   (
                                           sf.isnull('sourceZipCode') &
                                           sf.isnull('targetZipCode') &
                                           (sf.col('sourceCity') == sf.col('targetCity'))
                                   )
                                   )
                           .withColumn('street_lev_distance',
                                       sf.levenshtein(sf.col('sourceStreet'), sf.col('targetStreet')))
                           .filter(sf.col('street_lev_distance') < MIN_LEVENSHTEIN_DISTANCE)
                           )

    window = Window.partitionBy('concatId').orderBy(sf.desc('SIMILARITY'), 'ohubId')
    best_match = (similarity_filtered
                  .withColumn('rn', sf.row_number().over(window))
                  .filter(sf.col('rn') == 1)
                  .drop('rn')
                  .drop_duplicates()
                  # Join on name_index to get back the concatId and ohubId
                  .selectExpr('SIMILARITY',
                              'concatId',
                              'ohubId as ohubId_matched')
                  )

    return best_match


def postprocess_delta_operators(similarity: DataFrame,
                                ingested_preprocessed: DataFrame,
                                integrated_preprocessed: DataFrame):
    window = Window.partitionBy('i').orderBy(sf.desc('SIMILARITY'), 'j')
    best_match = (similarity
                  .withColumn('rn', sf.row_number().over(window))
                  .filter(sf.col('rn') == 1)
                  .drop('rn')
                  .drop_duplicates()
                  )

    # Join on name_index to get back the concatId and ohubId
    matched_ingested_daily = (best_match
                              .join(ingested_preprocessed, ingested_preprocessed['name_index'] == best_match['i'])
                              .drop('name_index')
                              .selectExpr('j', 'SIMILARITY',
                                          'matching_string as matching_string_new', 'concatId')
                              .join(integrated_preprocessed, sf.col('j') == integrated_preprocessed['name_index'])
                              .drop('name_index')
                              .selectExpr('SIMILARITY',
                                          'matching_string_new',
                                          'matching_string as matching_string_old',
                                          'concatId',
                                          'ohubId as ohubId_matched')
                              )
    return matched_ingested_daily


def recreate_matched_and_unmatched(integrated: DataFrame,
                                   ingested: DataFrame,
                                   matched: DataFrame):
    matched_ingested_daily_full_record = (matched
                                          .select('concatId', 'ohubId_matched')
                                          .join(ingested, on='concatId', how='left')
                                          .withColumn('ohubId', sf.col('ohubId_matched'))
                                          .drop('ohubId_matched')
                                          )

    updated_integrated = (integrated
                          .join(matched_ingested_daily_full_record, on='concatId', how='left_anti')
                          .union(matched_ingested_daily_full_record)
                          )

    unmatched = (ingested
                 .join(matched, on='concatId', how='left_anti')
                 )

    return (updated_integrated, unmatched)


def apply_delta_matching_on(spark,
                            ingested_records_for_country: DataFrame,
                            integrated_records_for_country: DataFrame,
                            preprocess_function,
                            postprocess_function,
                            n_top,
                            threshold):
    daily_preprocessed = (preprocess_function(ingested_records_for_country,
                                              'concatId', True)
                          .repartition('concatId')
                          .sort('concatId', ascending=True)
                          )
    integrated_preprocessed = (preprocess_function(integrated_records_for_country,
                                                   'ohubId', False)
                               .repartition('ohubId')
                               .sort('ohubId', ascending=True)
                               )

    similarity = match_delta_entity_for_country(spark,
                                                daily_preprocessed,
                                                integrated_preprocessed,
                                                n_top,
                                                threshold)
    matches = postprocess_function(similarity, daily_preprocessed, integrated_preprocessed)
    return_value = recreate_matched_and_unmatched(integrated_records_for_country,
                                                  ingested_records_for_country,
                                                  matches)
    return return_value


def main(arguments, preprocess_function, postprocess_function):
    """ Main function to start running the name matching. This does globally three things:

    * Read input files (two: integrated and ingested)
    * Name match between integrated and ingested
    * Write output files (two: one matched (with domain model schema), one unmatched (with domain model schema))

    If for some reason the matching was unable to run (due to data not being there, or too little data), an error is
    logged but the job is succesful

    Args:
        preprocess_function: Function to preprocess the data, one of
          `entity_matching.preprocess_operators`, `entity_matching.preprocess_contactpersons`
        postprocess_function: Function to postprocess the matching results, one of
          `postprocess_delta_contact_persons`, `postprocess_delta_operators`
    """
    global LOGGER
    spark, LOGGER = start_spark('Match and join newly ingested  with persistent ohubId')

    t = Timer('Reading for country {}'.format(arguments.country_code), LOGGER)
    ingested_daily = (spark.read.parquet(arguments.ingested_daily_input_path)
                      .filter(sf.col('countryCode') == arguments.country_code)
                      )

    integrated = (spark.read.parquet(arguments.integrated_input_path)
                  .filter(sf.col('countryCode') == arguments.country_code)
                  )
    ingested_daily.persist()
    integrated.persist()
    t.end_and_log()

    save_to_parquet_per_partition('countryCode', arguments.country_code)

    t = Timer('Running for country {}'.format(arguments.country_code), LOGGER)
    updated_ingegrated, unmatched = apply_delta_matching_on(spark,
                                                            ingested_daily,
                                                            integrated,
                                                            preprocess_function,
                                                            postprocess_function,
                                                            arguments.n_top,
                                                            arguments.threshold)

    t.end_and_log()
    if updated_ingegrated is None or unmatched is None:
        LOGGER.error('Matching was unable to run for country {}'.format(arguments.country_code))
    else:
        save_to_parquet_per_partition('countryCode', arguments.country_code)(updated_ingegrated,
                                                                             arguments.updated_integrated_output_path,
                                                                             'overwrite')
        save_to_parquet_per_partition('countryCode', arguments.country_code)(unmatched,
                                                                             arguments.unmatched_output_path,
                                                                             'overwrite')
