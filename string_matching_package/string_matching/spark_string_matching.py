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
- Return dataframe with row numbers pairs and their similarity value
"""

__author__ = "Rodrigo Agundez"
__version__ = "0.1"
__maintainer__ = "Rodrigo Agundez"
__email__ = "rodrigo.agundez@godatadriven.com"
__status__ = "Development"

import math
import numpy as np

import pyspark.sql.functions as sf

from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import IDF
from pyspark.ml.feature import NGram
from pyspark.ml.feature import Normalizer
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.types import ArrayType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from scipy.sparse import csr_matrix

from .sparse_dot_topn import sparse_dot_topn


VECTORIZE_STRING_COLUMN_NAME = 'vectorized_string'


def matrix_dot_limit(A, B, n_top,
                     threshold=0., start_row=0, upper_triangular=False):
    """Calculate dot product of sparse matrices

    This function uses a C++ wrapped in Cython implementation. It
    performs A x B and returns the row indices, column indices and
    value if value > `threshold` with a maximum of number of
    `n_top` matches.

    Since the function is optimized for speed it does not perform any
    type of checks on the input (like matrix dimensions).

    Args:
        A (Scipy csr Matrix): Left matrix of dimensions M x K
        B (Scipy csr Matrix): Right matrix of dimensions K x N
        n_top (int): Maximum number of matches to return
        threshold (float): Keep resulting values bigger then this value.
            [Default 0.0]
        start_row (int): First row index of matrix A. Useful when
            performing dot operation in chunks of A. [Default 0]
        upper_triangular (bool): If to return only the upper triangular
            matrix. This is useful when the task is deduplication.
            The upper triangular matrix is defined from the coordenate
            (start_row, start_row) in the resulting matrix.

    Returns:
        Generator[(int, int, float)]: Generator of tuples with
            (row_index, column_index, value).
    """
    B = B.tocsr()

    M = A.shape[0]
    N = B.shape[1]

    idx_dtype = np.int32

    if upper_triangular:
        # massive memory reduction
        # max number of possible non-zero element
        nnz_max = min(int(M * (2 * (N - start_row) - M - 1) / 2), M * n_top)
    else:
        nnz_max = M * n_top

    # arrays will be returned by reference
    rows = np.empty(nnz_max, dtype=idx_dtype)
    cols = np.empty(nnz_max, dtype=idx_dtype)
    data = np.empty(nnz_max, dtype=A.dtype)

    # C++ wrapped with Cython implementation
    # number of found non-zero entries in the upper triangular matrix
    # I'll use this value to slice the returning numpy array
    nnz = sparse_dot_topn(
        M, N,
        np.asarray(A.indptr, dtype=idx_dtype),
        np.asarray(A.indices, dtype=idx_dtype),
        A.data,
        np.asarray(B.indptr, dtype=idx_dtype),
        np.asarray(B.indices, dtype=idx_dtype),
        B.data,
        n_top,
        threshold,
        rows, cols, data, start_row, int(upper_triangular))

    return ((int(i), int(j), float(v)) for i, j, v in
            zip(rows[:nnz], cols[:nnz], data[:nnz]))


class StringVectorizer(object):
    """Pipeline to vectorize the strings in a column

    Uses different transformers and estimators
    """

    def __init__(self, input_col, *, n_gram, min_df, vocab_size):
        """Create vectorizer pipeline

        Args:
            n_gram: Granularity of n-grams to use e.g. 2 or 3
            min_dif: Minimum number of times a n-gram should appear
            vocab_size: Maximum number of n-grams to use
        """
        self.input_col = input_col
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
        regexTokenizer = RegexTokenizer(inputCol=self.input_col,
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
                                   outputCol=VECTORIZE_STRING_COLUMN_NAME,
                                   p=2)

        self.pipeline = Pipeline(
            stages=[regexTokenizer,
                    ngram_creator,
                    tf_counter,
                    idf_counter,
                    l2_normalizer]
        )

    def fit(self, df1, df2=None):
        """Fit transformers

        If two dataframes are sent then the vocabulary and document
        frequency are calculated usng the union.
        """
        df = df1.union(df2) if df2 else df1
        self.pipeline = self.pipeline.fit(df)
        return self

    def transform(self, df):
        """Apply all estimators.
        """
        return self.pipeline.transform(df)


def unpack_vector(sparse):
    """Combine indices and values into a tuples.

    For each value below 0.01 in the sparse vector we create a tuple and
    then add these tuples into a single list. The tuple contains the
    index and the value.
    """
    return ((int(index), float(value)) for index, value in
            zip(sparse.indices, sparse.values) if value > 0.01)


ngram_schema = ArrayType(StructType([
    StructField("ngram_index", IntegerType(), False),
    StructField("value", FloatType(), False)
]))


def dense_to_sparse_ddf(ddf, row_number_column):
    udf_unpack_vector = sf.udf(unpack_vector, ngram_schema)
    return (
        ddf
        .withColumn(
            'explode',
            sf.explode(udf_unpack_vector(sf.col(VECTORIZE_STRING_COLUMN_NAME)))
        )
        .withColumn('ngram_index', sf.col('explode').getItem('ngram_index'))
        .withColumn('value', sf.col('explode').getItem('value'))
        .select(row_number_column, 'ngram_index', 'value')
    )


def sparse_to_csr_matrix(ddf, row_number_column):
    df = ddf.toPandas()
    df[row_number_column] = df[row_number_column].astype(np.int32)
    df.ngram_index = df.ngram_index.astype(np.int32)
    df.value = df.value.astype(np.float64)

    csr_names_vs_ngrams = csr_matrix(
        (df.value.values,
         (df[row_number_column].values, df.ngram_index.values)),
        shape=(df[row_number_column].max() + 1, df.ngram_index.max() + 1),
        dtype=np.float64)
    del df
    return csr_names_vs_ngrams


def split_into_chunks(csr_names_vs_ngrams, matrix_chunks_rows):
    n_chunks = max(
        1, math.floor(csr_names_vs_ngrams.shape[0] / matrix_chunks_rows)
    )
    chunk_size = math.ceil(csr_names_vs_ngrams.shape[0] / n_chunks)
    n_chunks = math.ceil(csr_names_vs_ngrams.shape[0] / chunk_size)
    chunks = [
        (csr_names_vs_ngrams[
            (i * chunk_size):
            min((i + 1) * chunk_size, csr_names_vs_ngrams.shape[0])
        ], i * chunk_size)
        for i in range(n_chunks)]
    return chunks


similarity_schema = StructType([
    StructField("i", IntegerType(), False),
    StructField("j", IntegerType(), False),
    StructField("SIMILARITY", FloatType(), False)
])


def calculate_similarity(chunks_rdd, csr_rdd_transpose,
                         n_top, threshold, upper_triangular=False):
    """ Similarity is calculated in chunks
    """
    similarity = chunks_rdd.flatMap(
        lambda x: matrix_dot_limit(x[0], csr_rdd_transpose.value,
                                   n_top=n_top,
                                   threshold=threshold,
                                   start_row=x[1],
                                   upper_triangular=upper_triangular)
    )
    return similarity.toDF(similarity_schema)


def match_strings(spark, df,
                  *,
                  string_column,
                  row_number_column,
                  n_top,
                  threshold,
                  n_gram,
                  min_document_frequency,
                  max_vocabulary_size,
                  matrix_chunks_rows=500,
                  df2=None):

    str_vectorizer = (
        StringVectorizer(input_col=string_column,
                         n_gram=n_gram,
                         min_df=min_document_frequency,
                         vocab_size=max_vocabulary_size).fit(df, df2)
    )
    vectorized_strings = (
        str_vectorizer
        .transform(df)
        .select([row_number_column, VECTORIZE_STRING_COLUMN_NAME])
    )

    names_vs_ngrams = dense_to_sparse_ddf(vectorized_strings,
                                          row_number_column)
    csr_names_vs_ngrams = sparse_to_csr_matrix(names_vs_ngrams,
                                               row_number_column)

    if df2:
        upper_triangular = False
        vectorized_strings_2 = (
            str_vectorizer
            .transform(df2)
            .select([row_number_column, VECTORIZE_STRING_COLUMN_NAME])
        )

        names_vs_ngrams_2 = dense_to_sparse_ddf(vectorized_strings_2,
                                                row_number_column)
        csr_names_vs_ngrams_2 = sparse_to_csr_matrix(names_vs_ngrams_2,
                                                     row_number_column)
        csr_rdd_transpose = spark.sparkContext.broadcast(
            csr_names_vs_ngrams_2.transpose()
        )
    else:
        upper_triangular = True
        csr_rdd_transpose = spark.sparkContext.broadcast(
            csr_names_vs_ngrams.transpose()
        )

    chunks = split_into_chunks(csr_names_vs_ngrams, matrix_chunks_rows)
    chunks_rdd = spark.sparkContext.parallelize(chunks, numSlices=len(chunks))
    del csr_names_vs_ngrams

    similarity = calculate_similarity(chunks_rdd, csr_rdd_transpose,
                                      n_top, threshold, upper_triangular)

    return similarity
