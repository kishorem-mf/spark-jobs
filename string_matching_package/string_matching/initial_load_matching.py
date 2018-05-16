from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

from .utils import start_spark, Timer, read_parquet, get_country_codes, \
    save_to_parquet_per_partition, group_matches, select_and_repartition_country, \
    clean_operator_fields, create_operator_matching_string, clean_contactperson_fields, \
    create_contactperson_matching_string

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
    w = Window.partitionBy('countryCode').orderBy(sf.asc('id'))
    cleaned = clean_contactperson_fields(ddf, 'firstName', 'lastName', 'street', 'houseNumber', 'city', 'zipCode')

    filtered = (cleaned
                # keep only if no email and phone
                .filter(sf.isnull(sf.col('emailAddress')) & sf.isnull(sf.col('mobilePhoneNumber')))
                # drop if no first name and no last name
                .na.drop(subset=['firstNameCleansed', 'lastNameCleansed'], how='all')
                # drop if no street
                .na.drop(subset=['streetCleansed'], how='any')
                # same logic but for an empty string
                .filter((sf.col('streetCleansed') != '') &
                        ((sf.col('firstNameCleansed') != '') | (sf.col('lastNameCleansed') != '')))
                .withColumnRenamed('concatId', 'id')
                )

    return (create_contactperson_matching_string(filtered)
            .filter(sf.col('matching_string') != '')
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', 'id', 'matching_string', 'countryCode', 'firstNameCleansed', 'lastNameCleansed',
                    'streetCleansed', 'zipCodeCleansed', 'cityCleansed')
            )


def match_contacts_for_country(spark: SparkSession, country_code: str, preprocessed_contacts: DataFrame,
                               n_top: int, threshold: float):
    """Match contacts for a single country"""
    from .spark_string_matching import match_strings
    contacts = select_and_repartition_country(preprocessed_contacts, 'countryCode', country_code)
    similarity = match_strings(
        spark,
        df=contacts,
        string_column='matching_string',
        row_number_column='name_index',
        n_top=n_top,
        threshold=0.2,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE,
        matrix_chunks_rows=MATRIX_CHUNK_ROWS
    )
    similarity_filtered = join_contact_columns_and_filter(similarity, contacts, country_code)
    grouped_similarity = group_matches(similarity_filtered)
    return grouped_similarity


def join_contact_columns_and_filter(similarity: DataFrame, contacts: DataFrame, country_code: str) -> DataFrame:
    """Join back the original columns (street, zip, etc.) after matching and filter matches as follows:
        - keep only the matches with exactly matching zip code
            - if no zip code is present: keep match if cities (cleansed) match exactly
        - keep only the matches where Levenshtein distance between streets (cleansed) is lower than threshold (5)
    """
    return (similarity
            .join(contacts, similarity['i'] == contacts['name_index'],
                  how='left').drop('name_index')
            .selectExpr('i', 'j', 'id as sourceId',
                        'similarity', 'matching_string as sourceName',
                        'streetCleansed as sourceStreet',
                        'zipCodeCleansed as sourceZipCode',
                        'cityCleansed as sourceCity')
            .join(contacts, similarity['j'] == contacts['name_index'],
                  how='left').drop('name_index')
            .withColumn('countryCode', sf.lit(country_code))
            .selectExpr('i', 'j', 'countryCode', 'sourceId',
                        'id as targetId', 'similarity',
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
            .withColumn('street_lev_distance', sf.levenshtein(sf.col('sourceStreet'), sf.col('targetStreet')))
            .filter(sf.col('street_lev_distance') < MIN_LEVENSHTEIN_DISTANCE)
            )


def match_operators_for_country(spark: SparkSession, country_code: str, all_operators: DataFrame,
                                n_top: int, threshold: float):
    from .spark_string_matching import match_strings
    """Match operators for a single country"""
    operators = select_and_repartition_country(all_operators, 'countryCode', country_code)
    similarity = match_strings(
        spark,
        df=operators,
        string_column='matching_string',
        row_number_column='name_index',
        n_top=n_top,
        threshold=threshold,
        n_gram=N_GRAMS,
        min_document_frequency=MINIMUM_DOCUMENT_FREQUENCY,
        max_vocabulary_size=VOCABULARY_SIZE,
        matrix_chunks_rows=MATRIX_CHUNK_ROWS
    )
    grouped_similarity = group_matches(similarity)
    return join_operators_columns(grouped_similarity, operators, country_code)


def preprocess_operators(ddf: DataFrame) -> DataFrame:
    """Create a unique ID and the string that is used for matching and select only necessary columns"""
    w = Window.partitionBy('countryCode').orderBy(sf.asc('id'))
    ddf = clean_operator_fields(ddf, 'name', 'city', 'street', 'houseNumber', 'zipCode')

    ddf = (ddf.na.drop(subset=['nameCleansed'])
           .withColumnRenamed('concatId', 'id')
           .fillna(''))

    return (create_operator_matching_string(ddf)
            .filter(sf.col('matching_string') != '')
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', 'id', 'matching_string', 'countryCode')
            )


def join_operators_columns(grouped_similarity: DataFrame, operators: DataFrame, country_code: str) -> DataFrame:
    """Join back the original name and ID columns after matching"""
    return (grouped_similarity
            .join(operators, grouped_similarity['i'] == operators['name_index'],
                  how='left').drop('name_index')
            .selectExpr('i', 'j', 'id as sourceId',
                        'similarity', 'matching_string as sourceName')
            .join(operators, grouped_similarity['j'] == operators['name_index'],
                  how='left').drop('name_index')
            .withColumn('countryCode', sf.lit(country_code))
            .selectExpr('countryCode', 'sourceId', 'id as targetId',
                        'similarity', 'sourceName', 'matching_string as targetName'))


def apply_matching_on(records_per_country: DataFrame, spark, preprocess_function, match_function, country_code, n_top,
                      threshold):
    preprocessed = preprocess_function(records_per_country)
    country_codes = get_country_codes(country_code, preprocessed)
    return_value = None

    if len(country_codes) == 1:
        matches = match_function(spark, country_code, preprocessed, n_top, threshold)
        return_value = matches
    return return_value


def main(arguments, preprocess_function, match_function):
    global LOGGER
    spark, LOGGER = start_spark('Matching')

    t = Timer('Reading for country {}'.format(arguments.country_code), LOGGER)
    ddf = (read_parquet(spark, arguments.input_file, arguments.fraction)
           .filter(sf.col('countryCode') == arguments.country_code)
           )
    ddf.persist()
    t.end_and_log()

    t = Timer('Running for country {}'.format(arguments.country_code), LOGGER)
    grouped_matches = apply_matching_on(ddf, spark, preprocess_function, match_function, arguments.country_code,
                                        arguments.n_top, arguments.threshold)
    t.end_and_log()

    if grouped_matches is None:
        LOGGER.warn('Matching was unable to run due country {} too small'.format(arguments.country_code))
    else:
        save_to_parquet_per_partition('countryCode', arguments.country_code)(grouped_matches, arguments.output_path,
                                                                             'overwrite')
    ddf.unpersist()
