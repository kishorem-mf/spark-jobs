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
# ^ TODO: with 2 documents it cannot currently judge yet,
# as it throws out any n-grams occurring in either all documents or in only 1.
# should be related to TF-IDF giving 0.


def match_entity_for_country(spark: SparkSession,
                             entities: DataFrame,
                             n_top: int,
                             threshold: float,
                             min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY):
    """
    TODO describe function
    :param spark:
    :param entities:
    :param n_top:
    :param threshold:
    :return:
    """
    if entities.count() < MINIMUM_ENTRIES_PER_COUNTRY:
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
        min_document_frequency=min_document_frequency,
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


def postprocess_contact_persons(
        similarity: DataFrame,
        contacts: DataFrame,
        min_norm_name_levenshtein_sim=0.7,
        return_levenshtein_similarity=False):
    """
    Join back the original columns (street, zip, etc.) after matching and filter matches as follows:
    - Keep only the matches with exactly matching zip code
        - If no zip code is present: keep match if cities (cleansed) match exactly
    - Keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    :param pyspark.sql.DataFrame similarity: Spark DataFrame containing all similarities.
    :param pyspark.sql.DataFrame contacts: Spark DataFrame containing all contact persons.
    :param double min_norm_name_levenshtein_sim: Minimum normalised Levenshtein similarity for name column.
    :param bool return_levenshtein_similarity: True if column with Levenshtein similarity should be returned.
    :return:
    """
    similarity_filtered = (similarity
                           .join(contacts, similarity['i'] == contacts['name_index'],
                                 how='left').drop('name_index')
                           .selectExpr('i', 'j', 'concatId as sourceId',
                                       'similarity', 'matching_string as sourceName',
                                       'firstNameCleansed as sourceFirstName',
                                       'lastNameCleansed as sourceLastName',
                                       'streetCleansed as sourceStreet',
                                       'zipCodeCleansed as sourceZipCode',
                                       'cityCleansed as sourceCity')
                           .join(contacts, similarity['j'] == contacts['name_index'],
                                 how='left').drop('name_index')
                           .selectExpr('i', 'j', 'sourceId',
                                       'concatId as targetId', 'similarity',
                                       'sourceName', 'matching_string as targetName',
                                       'sourceFirstName',
                                       'sourceLastName',
                                       'firstNameCleansed as targetFirstName',
                                       'lastNameCleansed as targetLastName',
                                       'sourceStreet', 'streetCleansed as targetStreet',
                                       'sourceZipCode', 'zipCodeCleansed as targetZipCode',
                                       'sourceCity', 'cityCleansed as targetCity')
                           .filter(
                                   (sf.col('sourceZipCode') == sf.col('targetZipCode')) |
                                   (
                                        sf.isnull('sourceZipCode') &
                                        sf.isnull('targetZipCode') &
                                        (sf.col('sourceCity') == sf.col('targetCity'))
                                   )
                           )
                           .withColumn('street_lev_distance',
                                       sf.levenshtein(sf.col('sourceStreet'), sf.col('targetStreet')))
                           .filter(sf.col('street_lev_distance') < MIN_LEVENSHTEIN_DISTANCE)
                           .withColumn('norm_name_levenshtein_similarity',
                                       1 - sf.levenshtein(
                                            sf.concat(sf.col('sourceFirstName'), sf.col('sourceLastName')),
                                            sf.concat(sf.col('targetFirstName'), sf.col('targetLastName'))
                                       ) / sf.greatest(
                                            sf.length(sf.concat(sf.col('sourceFirstName'), sf.col('sourceLastName'))),
                                            sf.length(sf.concat(sf.col('targetFirstName'), sf.col('targetLastName')))
                                       )
                                       )
                           .where(~(
                                    # Exclude where (city,street&zipcode) are identical and normalised
                                    # Levenshtein similarity on name >= min_norm_name_levenshtein_sim
                                    (sf.col('sourceCity') == sf.col('targetCity')) &
                                    (sf.col('sourceStreet') == sf.col('targetCity')) &
                                    (sf.col('sourceZipCode') == sf.col('targetZipCode')) &
                                    (sf.col('norm_name_levenshtein_similarity') < min_norm_name_levenshtein_sim)
                                 ))
                           )

    grouped_similarity = group_matches(similarity_filtered)
    result_columns = ['i', 'j', 'sourceId', 'targetId', 'similarity', 'sourceName', 'targetName',
                      'sourceStreet', 'targetStreet', 'sourceZipCode', 'targetZipCode', 'sourceCity', 'targetCity']
    if return_levenshtein_similarity:
        result_columns.append('norm_name_levenshtein_similarity')

    return grouped_similarity.selectExpr(result_columns)


def preprocess_operators(ddf: DataFrame, id_column: str, drop_if_name_null=False) -> DataFrame:
    """
    Create a unique ID and the string that is used for matching and select only necessary columns.
    :param ddf:
    :param id_column:
    :param drop_if_name_null:
    :return:
    """
    # assumption: all use the same country code!
    w = Window.partitionBy('countryCode').orderBy(sf.asc(id_column))

    df = clean_operator_fields(ddf, 'name', 'city', 'street', 'houseNumber', 'zipCode')

    if drop_if_name_null:
        df = df.na.drop(subset=['nameCleansed'])

    return (create_operator_matching_string(df)
            .filter(sf.col('matching_string') != '')
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', id_column, 'matching_string', 'nameCleansed', 'cityCleansed', 'streetCleansed',
                    'zipCodeCleansed')
            )


def postprocess_operators(
        similarity: DataFrame,
        operators: DataFrame,
        min_norm_name_levenshtein_sim=0.7,
        return_levenshtein_similarity=False):
    """
    Join back the two dataframes containing (1) similarities and (2) operators.
    :param pyspark.sql.DataFrame similarity: Spark DataFrame containing all similarities.
    :param pyspark.sql.DataFrame operators: Spark DataFrame containing all operators.
    :param double min_norm_name_levenshtein_sim: Minimum normalised Levenshtein similarity for name column.
    :param bool return_levenshtein_similarity: True if column with Levenshtein similarity should be returned.
    :return:
    :rtype: pyspark.sql.DataFrame
    """
    grouped_similarity = group_matches(similarity)

    result = (grouped_similarity
              .join(operators, grouped_similarity['i'] == operators['name_index'], how='left')
              .selectExpr('j', 'i', 'similarity', 'concatId as sourceId', 'matching_string as sourceMatchingString',
                          'nameCleansed as sourceNameCleansed', 'cityCleansed as sourceCityCleansed',
                          'streetCleansed as sourceStreetCleansed', 'zipCodeCleansed as sourceZipCodeCleaned')
              .join(operators, grouped_similarity['j'] == operators['name_index'], how='left')
              .withColumn('norm_name_levenshtein_similarity',
                          1 - sf.levenshtein(sf.col('sourceNameCleansed'), sf.col('nameCleansed')) /
                          sf.greatest(sf.length('sourceNameCleansed'), sf.length('nameCleansed')))
              # Exclude where (city,street&zipcode) are identical and normalised
              # Levenshtein similarity on name >= min_norm_name_levenshtein_sim
              .where(~(
                  (sf.col('sourceCityCleansed') == sf.col('cityCleansed')) &
                  (sf.col('sourceStreetCleansed') == sf.col('streetCleansed')) &
                  (sf.col('sourceZipCodeCleaned') == sf.col('zipCodeCleansed')) &
                  (sf.col('norm_name_levenshtein_similarity') < min_norm_name_levenshtein_sim)))
              )

    result_columns = ['sourceId', 'concatId as targetId', 'similarity', 'sourceMatchingString as sourceName',
                      'matching_string as targetName']
    if return_levenshtein_similarity:
        result_columns.append('norm_name_levenshtein_similarity')

    return result.selectExpr(result_columns)


def apply_operator_matching(*args, **kwargs):
    return apply_matching_on(
        *args,
        preprocess_function=preprocess_operators,
        postprocess_function=postprocess_operators,
        min_document_frequency=1,  # override apply_matching_on default for tests
        **kwargs)


def apply_contactperson_matching(*args, **kwargs):
    return apply_matching_on(
        *args,
        preprocess_function=preprocess_contact_persons,
        postprocess_function=postprocess_contact_persons,
        min_document_frequency=1,  # override apply_matching_on default for tests
        **kwargs)


def apply_matching_on(
    records_per_country: DataFrame,
    spark,
    preprocess_function=preprocess_operators,
    postprocess_function=postprocess_operators,
    n_top=1500,
    threshold=0.8,
    min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
    min_norm_name_levenshtein_sim=0.7,
    return_levenshtein_similarity: bool = False
):
    """
    Apply similarity matching on given DataFrame.
    This function is the glue doing preprocessing - matching - postprocessing.
    :param pyspark.sql.DataFrame records_per_country:
    :param pyspark.sql.SparkSession spark:
    :param callable preprocess_function:
    :param int n_top:
    :param double threshold:
    :param double min_norm_name_levenshtein_sim:
    :param bool return_levenshtein_similarity:
    :return:
    :rtype: pyspark.sql.DataFrame
    """
    preprocessed = (
        preprocess_function(records_per_country, "concatId", True)
        .repartition("concatId")
        .sort("concatId", ascending=True)
    )

    similarity = match_entity_for_country(spark, preprocessed, n_top, threshold, min_document_frequency)

    matches = postprocess_function(
        similarity,
        preprocessed,
        min_norm_name_levenshtein_sim=min_norm_name_levenshtein_sim,
        return_levenshtein_similarity=return_levenshtein_similarity,
    )

    return matches


def main_operators(arguments):
    return main(arguments, preprocess_operators, postprocess_operators)


def main_contactpersons(arguments):
    return main(arguments, preprocess_contact_persons, postprocess_contact_persons)


def main(arguments, preprocess_function, post_process_function,
         min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY):
    """
    Main function to start running the name matching. This overall does three things:

    1. Read input file (one: ingested)
    2. Name match between ingested and ingested (cartesian product)
    3. Write output files (one: one matched (with similarity schema))

    If for some reason the matching was unable to run (due to data not being there, or too little data), an error is
    logged but the job is successful.

    :param arguments:
    :param callable preprocess_function: Function to preprocess the data, one of
        `preprocess_operators`, `preprocess_contactpersons`.
    :param callable post_process_function: Function to postprocess the matching results, one of
            `postprocess_contact_persons`, `postprocess_operators`.
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
    grouped_matches = apply_matching_on(
                        ddf,
                        spark,
                        preprocess_function,
                        post_process_function,
                        arguments.n_top,
                        arguments.threshold,
                        min_document_frequency=min_document_frequency,
                        min_norm_name_levenshtein_sim=arguments.min_norm_name_levenshtein_sim)
    t.end_and_log()

    if grouped_matches is None:
        LOGGER.warn('Matching was unable to run due country {} too small'.format(arguments.country_code))
    else:
        save_to_parquet_per_partition('countryCode', arguments.country_code)(grouped_matches, arguments.output_path,
                                                                             'overwrite')
    ddf.unpersist()
