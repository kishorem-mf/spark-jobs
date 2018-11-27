from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

from .spark_string_matching import similarity_schema
from .utils import (
    start_spark,
    Timer,
    read_parquet,
    save_to_parquet_per_partition,
    group_matches,
    clean_operator_fields,
    create_operator_matching_string,
    clean_contactperson_fields,
    create_contactperson_matching_string,
)

MATRIX_CHUNK_ROWS = 500
N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
VOCABULARY_SIZE = 1500
MIN_LEVENSHTEIN_DISTANCE = 5
LOGGER = None
MINIMUM_ENTRIES_PER_COUNTRY = 3


def match_entity_for_country(spark: SparkSession,
                             entities: DataFrame,
                             n_top: int,
                             threshold: float):
    """
    TODO describe function
    :param spark:
    :param entities:
    :param n_top:
    :param threshold:
    :return:
    """
    if not (entities.count() >= MINIMUM_ENTRIES_PER_COUNTRY):
        return spark.createDataFrame([], similarity_schema)

    from .spark_string_matching import match_strings
    """Match entities for a single country"""
    similarity = match_strings(
        spark,
        df=entities,
        string_column='matching_string',
        row_number_column='name_index',
        n_top=n_top,
        threshold=threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE,
        matrix_chunks_rows=MATRIX_CHUNK_ROWS
    )
    return similarity


def preprocess_contact_persons(ddf: DataFrame, id_column: str, *args) -> DataFrame:
    """
    Some pre-processing
        - Keep only contacts without e-mail AND without mobile phone number
        - Remove contacts without first AND without last name (cleansed)
        - Remove contacts without a street (cleansed)
        - Create a unique ID
        - Create matching-string
        - Select only necessary columns
    :param ddf:
    :param id_column:
    :param args:
    :return:
    """
    w = Window.partitionBy('countryCode').orderBy(sf.asc(id_column))
    cleaned = clean_contactperson_fields(ddf, 'firstName', 'lastName', 'street', 'houseNumber', 'city', 'zipCode')

    filtered = (cleaned
                # keep only if no email and phone
                .filter(sf.isnull(sf.col('emailAddress')) & sf.isnull(sf.col('mobileNumber')))
                # drop if no first name and no last name
                .na.drop(subset=['firstNameCleansed', 'lastNameCleansed'], how='all')
                # drop if no street
                .na.drop(subset=['streetCleansed'], how='any')
                # same logic but for an empty string
                .filter((sf.col('streetCleansed') != '') &
                        ((sf.col('firstNameCleansed') != '') | (sf.col('lastNameCleansed') != '')))
                )

    return (create_contactperson_matching_string(filtered)
            .filter(sf.col('matching_string') != '')
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', id_column, 'matching_string', 'firstNameCleansed', 'lastNameCleansed',
                    'streetCleansed', 'zipCodeCleansed', 'cityCleansed')
            )


def postprocess_contact_persons(similarity: DataFrame, contacts: DataFrame):
    """
    Join back the original columns (street, zip, etc.) after matching and filter matches as follows:
    - Keep only the matches with exactly matching zip code
        - If no zip code is present: keep match if cities (cleansed) match exactly
    - Keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    :param similarity:
    :param contacts:
    :return:
    """
    similarity_filtered = (similarity
                           .join(contacts, similarity['i'] == contacts['name_index'],
                                 how='left').drop('name_index')
                           .selectExpr('i', 'j', 'concatId as sourceId',
                                       'similarity', 'matching_string as sourceName',
                                       'streetCleansed as sourceStreet',
                                       'zipCodeCleansed as sourceZipCode',
                                       'cityCleansed as sourceCity')
                           .join(contacts, similarity['j'] == contacts['name_index'],
                                 how='left').drop('name_index')
                           .selectExpr('i', 'j', 'sourceId',
                                       'concatId as targetId', 'similarity',
                                       'sourceName', 'matching_string as targetName',
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
    grouped_similarity = group_matches(similarity_filtered)
    return grouped_similarity


def preprocess_operators(ddf: DataFrame, id_column: str, drop_if_name_null=False) -> DataFrame:
    """
    Create a unique ID and the string that is used for matching and select only necessary columns.
    :param ddf:
    :param id_column:
    :param drop_if_name_null:
    :return:
    """
    w = Window.partitionBy('countryCode').orderBy(sf.asc(id_column))
    ddf = clean_operator_fields(ddf, 'name', 'city', 'street', 'houseNumber', 'zipCode')

    if drop_if_name_null:
        ddf = ddf.na.drop(subset=['nameCleansed'])

    return (create_operator_matching_string(ddf)
            .filter(sf.col('matching_string') != '')
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', id_column, 'matching_string')
            )


def postprocess_operators(similarity: DataFrame, operators: DataFrame):
    """
    TOOD describe function
    :param similarity:
    :param operators:
    :return:
    """
    grouped_similarity = group_matches(similarity)

    return (grouped_similarity
            .join(operators, grouped_similarity['i'] == operators['name_index'],
                  how='left').drop('name_index')
            .selectExpr('i', 'j', 'concatId as sourceId',
                        'similarity', 'matching_string as sourceName')
            .join(operators, grouped_similarity['j'] == operators['name_index'],
                  how='left').drop('name_index')
            .selectExpr('sourceId', 'concatId as targetId',
                        'similarity', 'sourceName', 'matching_string as targetName'))


def apply_matching_on(records_per_country: DataFrame, spark,
                      preprocess_function,
                      post_process_function,
                      n_top,
                      threshold):
    """
    TODO describe function
    :param records_per_country:
    :param spark:
    :param preprocess_function:
    :param post_process_function:
    :param n_top:
    :param threshold:
    :return:
    """
    preprocessed = (preprocess_function(records_per_country, 'concatId', True)
                    .repartition('concatId')
                    .sort('concatId', ascending=True)
                    )

    similarity = match_entity_for_country(spark, preprocessed, n_top, threshold)
    matches = post_process_function(similarity, preprocessed)
    return_value = matches
    return return_value


def main(arguments, preprocess_function, post_process_function):
    """
    Main function to start running the name matching. This does globally three things:

    1. Read input file (one: ingested)
    2. Name match between ingested and ingested (cartesian product)
    3. Write output files (one: one matched (with similarity schema))

    If for some reason the matching was unable to run (due to data not being there, or too little data), an error is
    logged but the job is successful.

    :param arguments:
    :param callable preprocess_function: Function to preprocess the data, one of `preprocess_operators`, `preprocess_contactpersons`.
    :param callable post_process_function: Function to postprocess the matching results, one of `postprocess_contact_persons`, `postprocess_operators`.
    :return:
    """
    global LOGGER
    spark, LOGGER = start_spark('Matching')

    t = Timer('Reading for country {}'.format(arguments.country_code), LOGGER)
    ddf = (read_parquet(spark, arguments.input_file)
           .filter(sf.col('countryCode') == arguments.country_code)
           )
    ddf.persist()
    t.end_and_log()

    t = Timer('Running for country {}'.format(arguments.country_code), LOGGER)
    grouped_matches = apply_matching_on(ddf, spark,
                                        preprocess_function,
                                        post_process_function,
                                        arguments.n_top,
                                        arguments.threshold)
    t.end_and_log()

    if grouped_matches is None:
        LOGGER.warn('Matching was unable to run due country {} too small'.format(arguments.country_code))
    else:
        save_to_parquet_per_partition('countryCode', arguments.country_code)(grouped_matches, arguments.output_path,
                                                                             'overwrite')
    ddf.unpersist()
