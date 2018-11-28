from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from string_matching import spark_string_matching


def gen_tuple(tup, n, start_idx=0):
    return [(str(i),) + tup[1:-1] + (tup[-1].format(idx=(i + start_idx)),) for i in range(n)]


class TestMatchStrings:

    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('id', 'matching_string')

    def test_matching_on_single_dataframe(self, spark):
        data = [(0, 'ab'),
                (1, 'ac'),
                (2, 'ab'),
                ]
        ddf = self.create_ddf(spark, data)
        res = (
            spark_string_matching.match_strings(
                spark,
                df=ddf,
                string_column="matching_string",
                row_number_column="id",
                n_top=1500,
                threshold=0.8,
                n_gram=2,
                min_document_frequency=2,
                max_vocabulary_size=1500,
            )
                .select("i", "j")
                .sort("j", "i")
                .collect()
        )
        assert len(res) == 1
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == [0]
        assert targets == [2]

    def test_matching_on_two_dataframes(self, spark):
        data_delta = [(0, 'ab'),
                      (1, 'ac'),
                      (2, 'ab'),
                      (3, 'ae'),
                      ]
        data_integrated = [(0, 'ab'),
                           (1, 'ab'),
                           (2, 'ae'),
                           (3, 'ad'),
                           ]

        ddf_delta = self.create_ddf(spark, data_delta)
        ddf_integrated = self.create_ddf(spark, data_integrated)
        res = (
            spark_string_matching.match_strings(
                spark,
                df=ddf_delta,
                df2=ddf_integrated,
                string_column="matching_string",
                row_number_column="id",
                n_top=1500,
                threshold=0.8,
                n_gram=2,
                min_document_frequency=2,
                max_vocabulary_size=1500,
            )
                .select("i", "j")
                .sort("i", "j")
                .collect()
        )  # i is ingested
        assert len(res) == 5
        ingested = [_[0] for _ in res]
        integrated = [_[1] for _ in res]
        assert ingested == [0, 0, 2, 2, 3]
        assert integrated == [0, 1, 0, 1, 2]

    def test_matching_on_sparse_dataframe(self, spark):
        data = [(0, 'aa'),
                (1, 'ab'),
                (2, 'ac'),
                ]
        ddf = self.create_ddf(spark, data)
        res = (
            spark_string_matching.match_strings(
                spark,
                df=ddf,
                string_column="matching_string",
                row_number_column="id",
                n_top=1500,
                threshold=0.8,
                n_gram=2,
                min_document_frequency=2,
                max_vocabulary_size=1500,
            )
                .select("i", "j")
                .sort("j", "i")
                .collect()
        )
        assert len(res) == 0

    def test_matching_on_sparse_second_dataframe(self, spark):
        data_delta = [(0, 'ab'),
                      (1, 'ac'),
                      (2, 'ab'),
                      (3, 'ae'),
                      ]
        data_integrated = [(0, 'xx'),
                           (1, 'xy'),
                           (2, 'xz'),
                           ]

        ddf_delta = self.create_ddf(spark, data_delta)
        ddf_integrated = self.create_ddf(spark, data_integrated)
        res = (
            spark_string_matching.match_strings(
                spark,
                df=ddf_delta,
                df2=ddf_integrated,
                string_column="matching_string",
                row_number_column="id",
                n_top=1500,
                threshold=0.8,
                n_gram=2,
                min_document_frequency=2,
                max_vocabulary_size=1500,
            )
                .select("i", "j")
                .sort("j", "i")
                .collect()
        )
        assert len(res) == 0

    def test_match_strings(self, spark):
        df = spark.createDataFrame(
            data=[
                (0, 0, "brownthornton ebonybury abbott rue6 07172"),
                (1, 1, "washington group katelynville debbie ville8 30336"),
                (2, 2, "holloway llc dannyfort mann pines 48296"),
                (3, 3, "owenthomas eastdarinfurt penny plaza 73947"),
                (4, 4, "cobbnichols darrellmouth jeffery extension4 61626"),
            ],
            schema=StructType(
                [
                    StructField("name_index", IntegerType(), True),
                    StructField("concatId", StringType(), True),
                    StructField("matching_string", StringType(), True),
                ]
            ),
        )

        similarities = spark_string_matching.match_strings(
            spark=spark,
            df=df,
            string_column="matching_string",
            row_number_column="name_index",
            n_top=1500,
            threshold=0,
            n_gram=2,
            min_document_frequency=2,
            max_vocabulary_size=1500,
            matrix_chunks_rows=500,
        )

        # n records should result in n-1 th triangular number similarities.
        # E.g. 5 records should result in 4+3+2+1=10 similarities.
        assert similarities.count() == sum(range(df.count()))