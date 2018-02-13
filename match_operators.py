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

import os
import argparse
from typing import List
from time import perf_counter as timer

import math
import numpy as np

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, IDF, NGram, \
    Normalizer, RegexTokenizer
from pyspark.sql import SparkSession, DataFrame, functions as sf
from pyspark.sql.types import ArrayType, FloatType, IntegerType, \
    LongType, StructField, StructType, StringType
from pyspark.sql.window import Window

from scipy.sparse import csr_matrix

__author__ = "Rodrigo Agundez"
__version__ = "0.1"
__maintainer__ = "Rodrigo Agundez"
__email__ = "rodrigo.agundez@godatadriven.com"
__status__ = "Development"

EGG_NAME = 'sparse_dot_topn.egg'
MININUM_OPERATORS_PER_COUNTRY = 100
MATRIX_CHUNK_ROWS = 500
N_GRAMS = 2
MINIMUM_DOCUMENT_FREQUENCY = 2
VOCABULARY_SIZE = 1500

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
# some names which appear a lot of times (> 2000) and contain no
# specific meaning
DROP_NAMES = ['unknown',
              'unknown companyad',
              'not specified notspecified not specified0 00000',
              '是否 54534',
              'privat']

ngram_schema = ArrayType(StructType([
    StructField("ngram_index", IntegerType(), False),
    StructField("value", FloatType(), False)
]))

similarity_schema = StructType([
    StructField("i", IntegerType(), False),
    StructField("j", IntegerType(), False),
    StructField("SIMILARITY", FloatType(), False)
])


def start_spark():
    """
    # assigning at least 4GB to each core
    # NOTE: HDInsight by default allocates a single core per executor
    # regardless of what we set in here. The truth is in Yarn UI not in Spark UI.
    """
    spark = (SparkSession
             .builder
             .appName("NameMatching")
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
    dir_path = os.path.dirname(os.path.realpath(__file__))
    sc.addPyFile(os.path.join(dir_path, 'dist', EGG_NAME))

    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().getLogger('org').setLevel(log4j.Level.WARN)
    log4j.LogManager.getRootLogger().getLogger('akka').setLevel(log4j.Level.ERROR)

    global LOGGER
    LOGGER = log4j.LogManager.getLogger('Name Matching')
    return spark


def chunk_dot_limit(A, B, ntop,
                    threshold=0, start_row=0, upper_triangular=False):
    """Calculate dot product of sparse matrices

    This function uses a C++ wrapped in Cython implementation. It
    performs A x B and returns the row, column indices and value if the
    value `threshold` with a maximum of matches of `ntop`.

    It will return the upper triangular matrix.

    Args:
        A (Scipy csr Matrix): Right matrix of dimensions M x K
        B (Scipy csr Matrix): Let matrix of dimensions K x M
        ntop (int): Maximum number of matches to return
        threshold (float): Minimum value of any cell positions to return.
        start_row (int): Assign the first row number of the matrix to
            this value.

    Returns:
        Generator[(int, int, float)]: Generator of tuples with
            (row_index, column_index, value).
    """
    import sparse_dot_topn.sparse_dot_topn as ct
    B = B.tocsr()

    M = A.shape[0]
    N = B.shape[1]

    idx_dtype = np.int32

    if upper_triangular:
        # massive memory reduction
        # max number of possible non-zero element
        nnz_max = min(int(M * (2 * (N - start_row) - M - 1) / 2), M * ntop)
    else:
        nnz_max = M * ntop

    # arrays will be returned by reference
    rows = np.empty(nnz_max, dtype=idx_dtype)
    cols = np.empty(nnz_max, dtype=idx_dtype)
    data = np.empty(nnz_max, dtype=A.dtype)

    # C++ wrapped with Cython implementation
    # number of found non-zero entries in the upper triangular matrix
    # I'll use this value to slice the returning numpy array
    nnz = ct.sparse_dot_topn(
        M, N,
        np.asarray(A.indptr, dtype=idx_dtype),
        np.asarray(A.indices, dtype=idx_dtype),
        A.data,
        np.asarray(B.indptr, dtype=idx_dtype),
        np.asarray(B.indices, dtype=idx_dtype),
        B.data,
        ntop,
        threshold,
        rows, cols, data, start_row, int(upper_triangular))

    return ((int(i), int(j), float(v)) for i, j, v in
            zip(rows[:nnz], cols[:nnz], data[:nnz]))


class NameVectorizer(object):
    """Pipeline to vectorize the strings in a column

    Uses different transformers and estimators
    """

    def __init__(self, n_gram, min_df, vocab_size):
        """Create vectorizer pipeline

        Args:
            n_gram: Granularity of n-grams to use e.g. 2 or 3
            min_dif: Minimum number of times a n-gram should appear
            vocab_size: Maximum number of n-grams to use
        """
        self.n_gram = n_gram
        self.min_df = min_df
        self.vocab_size = vocab_size
        self.__create_pipeline()

    def __create_pipeline(self):
        """Initialize components and add them to a Pipeline object

        - RegexTokenizer separates each string into a list of characters
        - NGram creates n-grams from the list of characters
        - CountVectorizer calculates n-gram frequency on the strings
        - IDF calculates inverse n-gram frequency on the corpus
        - Normalizer 2-norm normalized the resulting vector of each string
        """
        regexTokenizer = RegexTokenizer(inputCol="name",
                                        outputCol="tokens",
                                        pattern="")
        ngram_creator = NGram(inputCol="tokens",
                              outputCol="n_grams",
                              n=self.n_gram)
        tf_counter = CountVectorizer(inputCol='n_grams',
                                     outputCol='term_frequency',
                                     minTF=1.0,
                                     minDF=self.min_df,
                                     vocabSize=self.vocab_size,
                                     binary=False)
        idf_counter = IDF(inputCol="term_frequency",
                          outputCol="tfidf_vector")
        l2_normalizer = Normalizer(inputCol="tfidf_vector",
                                   outputCol="name_vector",
                                   p=2)

        self.pipeline = Pipeline(
            stages=[regexTokenizer,
                    ngram_creator,
                    tf_counter,
                    idf_counter,
                    l2_normalizer]
        )

    def fit_transform(self, df):
        """Fit transformers and apply all estimators.
        """
        return self.pipeline.fit(df).transform(df)


def unpack_vector(sparse):
    """Combine indices and values into a tuples.

    For each value below 0.1 in the sparse vector we create a tuple and
    then add these tuples into a single list. The tuple contains the
    index and the value.
    """
    return ((int(index), float(value)) for index, value in
            zip(sparse.indices, sparse.values) if value > 0.05)


def read_operators(spark: SparkSession, fn: str, fraction: float) -> DataFrame:
    return (spark
            .read
            .parquet(fn)
            .sample(False, fraction))


def preprocess_operators(ddf: DataFrame) -> DataFrame:
    w = Window.partitionBy('COUNTRY_CODE').orderBy(sf.asc('id'))
    return (ddf
            .na.drop(subset=['NAME_CLEANSED'])
            # create unique ID
            .withColumn('id', sf.concat_ws('~',
                                           sf.col('COUNTRY_CODE'),
                                           sf.col('SOURCE'),
                                           sf.col('REF_OPERATOR_ID')))
            .fillna('')
            # create string columns to matched
            .withColumn('name',
                        sf.concat_ws(' ',
                                     sf.col('NAME_CLEANSED'),
                                     sf.col('CITY_CLEANSED'),
                                     sf.col('STREET_CLEANSED'),
                                     sf.col('ZIP_CODE_CLEANSED')))
            .withColumn('name', sf.regexp_replace('name', REGEX, ''))
            .withColumn('name', sf.trim(sf.regexp_replace('name', '\s+', ' ')))
            # .filter(~sf.col('name').isin(*DROP_NAMES))
            .withColumn('name_index', sf.row_number().over(w) - 1)
            .select('name_index', 'id', 'name', 'COUNTRY_CODE'))


def get_country_codes(country_code_arg: str, operators: DataFrame) -> List[str]:
    if country_code_arg == 'all':
        opr_count = operators.groupby('COUNTRY_CODE').count()
        LOGGER.info("Selecting countries with more than " + str(MININUM_OPERATORS_PER_COUNTRY) + " entries")
        codes = (
            opr_count[opr_count['count'] > MININUM_OPERATORS_PER_COUNTRY]
            .select('COUNTRY_CODE')
            .distinct()
            .rdd.map(lambda r: r[0]).collect())
    else:
        LOGGER.info("Selecting only country: " + country_code_arg)
        codes = [args.country_code]
    return codes


def select_and_repartition_country(ddf: DataFrame, country_code: str) -> DataFrame:
    return (ddf
            .filter(sf.col('COUNTRY_CODE') == country_code)
            .drop('COUNTRY_CODE')
            .repartition('id')
            .sort('id', ascending=True))


def dense_to_sparse_ddf(ddf: DataFrame) -> DataFrame:
    udf_unpack_vector = sf.udf(unpack_vector, ngram_schema)
    return (ddf
            .withColumn('explode', sf.explode(udf_unpack_vector(sf.col('name_vector'))))
            .withColumn('ngram_index', sf.col('explode').getItem('ngram_index'))
            .withColumn('value', sf.col('explode').getItem('value'))
            .select('name_index', 'ngram_index', 'value'))


def sparse_to_csr_matrix(ddf: DataFrame) -> csr_matrix:
    LOGGER.info('Load names vs ngrams value to Pandas')
    df = ddf.toPandas()
    df.name_index = df.name_index.astype(np.int32)
    df.ngram_index = df.ngram_index.astype(np.int32)
    df.value = df.value.astype(np.float64)

    csr_names_vs_ngrams = csr_matrix(
        (df.value.values, (df.name_index.values, df.ngram_index.values)),
        shape=(df.name_index.max() + 1, df.ngram_index.max() + 1),
        dtype=np.float64)
    del df
    return csr_names_vs_ngrams


def split_into_chunks(csr_names_vs_ngrams: csr_matrix):
    n_chunks = max(1, math.floor(csr_names_vs_ngrams.shape[0] / MATRIX_CHUNK_ROWS))
    chunk_size = math.ceil(csr_names_vs_ngrams.shape[0] / n_chunks)
    LOGGER.info("Matrix chunk size is " + str(chunk_size))
    n_chunks = math.ceil(csr_names_vs_ngrams.shape[0] / chunk_size)
    chunks = [(csr_names_vs_ngrams[
               (i * chunk_size): min((i + 1) * chunk_size, csr_names_vs_ngrams.shape[0])], i * chunk_size)
              for i in range(n_chunks)]
    return chunks


def calculate_similarity(chunks_rdd, csr_rdd_transpose) -> DataFrame:
    """
    Similarity is calculated in chunks
    """
    similarity = chunks_rdd.flatMap(
        lambda x: chunk_dot_limit(x[0], csr_rdd_transpose.value,
                                  ntop=args.ntop,
                                  threshold=args.threshold,
                                  start_row=x[1],
                                  upper_triangular=True)
    )

    return similarity.toDF(similarity_schema)


def group_similarities(similarity: DataFrame) -> DataFrame:
    """
    group similarities with column i being the group id
    join name_index with original unique id
    """
    grouping_window = (
        Window
        .partitionBy('j')
        .orderBy(sf.asc('i')))

    # keep only the first entry sorted alphabetically
    grp_sim = (
        similarity
        .withColumn("rn", sf.row_number().over(grouping_window))
        .filter(sf.col("rn") == 1)
        .drop('rn')
    )

    # remove group ID from column j
    return grp_sim.join(
        grp_sim.select('j').subtract(grp_sim.select('i')),
        on='j', how='inner'
    )


def find_matches(grouped_similarity: DataFrame, operators: DataFrame, country_code: str) -> DataFrame:
    return (grouped_similarity
            .join(operators, grouped_similarity['i'] == operators['name_index'],
                  how='left').drop('name_index')
            .selectExpr('i', 'j', 'id as SOURCE_ID',
                        'SIMILARITY', 'name as SOURCE_NAME')
            .join(operators, grouped_similarity['j'] == operators['name_index'],
                  how='left').drop('name_index')
            .withColumn('COUNTRY_CODE', sf.lit(country_code))
            .selectExpr('COUNTRY_CODE', 'SOURCE_ID', 'id as TARGET_ID',
                        'SIMILARITY', 'SOURCE_NAME', 'name as TARGET_NAME'))


def save_to_parquet(matches: DataFrame, fn):
    mode = 'append'
    LOGGER.info("Writing to: " + fn)
    LOGGER.info("Mode: " + mode)

    (matches
        .coalesce(20)
        .write
        .partitionBy('country_code')
        .parquet(fn, mode=mode))


def print_stats(matches: DataFrame):
    matches.persist()
    n_matches = matches.count()

    print('\n\nNr. Similarities:\t', n_matches)
    print('Threshold:\t', args.threshold)
    print('NTop:\t', args.ntop)
    (matches
        .select('SOURCE_ID', 'TARGET_ID',
                'SIMILARITY', 'SOURCE_NAME', 'TARGET_NAME')
        .sort('SIMILARITY', ascending=True)
        .show(50, truncate=False))

    (matches
        .groupBy(['SOURCE_ID', 'SOURCE_NAME'])
        .count()
        .sort('count', ascending=False).show(50, truncate=False))

    matches.describe('SIMILARITY').show()


def name_match_country_operators(spark: SparkSession, country_code: str, all_operators: DataFrame):
    LOGGER.info("Creating row id for country: " + country_code)
    # get country data and add row number column
    operators = select_and_repartition_country(all_operators, country_code)

    name_vectorizer = NameVectorizer(n_gram=N_GRAMS,
                                     min_df=MINIMUM_DOCUMENT_FREQUENCY,
                                     vocab_size=VOCABULARY_SIZE)
    encoded_names = (
        name_vectorizer
        .fit_transform(operators)
        .select(['name_index', 'name_vector']))

    names_vs_ngrams = dense_to_sparse_ddf(encoded_names)
    csr_names_vs_ngrams = sparse_to_csr_matrix(names_vs_ngrams)

    LOGGER.info("Broadcasting sparse matrix")
    csr_rdd_transpose = spark.sparkContext.broadcast(csr_names_vs_ngrams.transpose())

    chunks = split_into_chunks(csr_names_vs_ngrams)
    LOGGER.info("Parallelizing matrix in " + str(len(chunks)) + " chunks")
    chunks_rdd = spark.sparkContext.parallelize(chunks, numSlices=len(chunks))

    del csr_names_vs_ngrams

    similarity = calculate_similarity(chunks_rdd, csr_rdd_transpose)
    grouped_similarity = group_similarities(similarity)
    matches = find_matches(grouped_similarity, operators, country_code)

    if args.output_path:
        save_to_parquet(matches, args.output_path)
    else:
        print_stats(matches)


def main(args):
    spark = start_spark()

    t = Timer('Preprocessing operators', LOGGER)
    all_operators = read_operators(spark, args.input_file, args.fraction)
    preprocessed_operators = preprocess_operators(all_operators)

    LOGGER.info("Parsing and persisting operator data")
    preprocessed_operators.persist()
    t.end_and_log()

    country_codes = get_country_codes(args.country_code, preprocessed_operators)

    for country_code in country_codes:
        t = Timer('Running for country {}'.format(country_code), LOGGER)
        name_match_country_operators(spark, country_code, preprocessed_operators)
        t.end_and_log()


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
    parser.add_argument('-n', '--ntop', default=1500, type=int,
                        help='keep N top similarities for each record.')
    args = parser.parse_args()

    main(args)
