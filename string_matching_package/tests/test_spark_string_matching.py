from string_matching import spark_string_matching as victim


def gen_tuple(tup, n, start_idx=0):
    return [(str(i),) + tup[1:-1] + (tup[-1].format(idx=(i + start_idx)),) for i in range(n)]


class TestMatchStrings(object):

    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('id', 'matching_string')

    def test_matching_on_single_dataframe(self, spark):
        data = [(0, 'ab'),
                (1, 'ac'),
                (2, 'ab'),
                ]
        ddf = self.create_ddf(spark, data)
        res = victim.match_strings(spark,
                                   df=ddf,
                                   string_column='matching_string',
                                   row_number_column='id',
                                   n_top=1500,
                                   threshold=0.8,
                                   n_gram=2,
                                   min_document_frequency=2,
                                   max_vocabulary_size=1500).select('i', 'j').sort('j', 'i').collect()
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
        res = victim.match_strings(spark,
                                   df=ddf_delta,
                                   df2=ddf_integrated,
                                   string_column='matching_string',
                                   row_number_column='id',
                                   n_top=1500,
                                   threshold=0.8,
                                   n_gram=2,
                                   min_document_frequency=2,
                                   max_vocabulary_size=1500).select('i', 'j').sort('i', 'j').collect()  # i is ingested
        assert len(res) == 5
        ingested = [_[0] for _ in res]
        integrated = [_[1] for _ in res]
        assert ingested == [0, 0, 2, 2, 3]
        assert integrated == [0, 1, 0, 1, 2]

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
        res = victim.match_strings(spark,
                                   df=ddf_delta,
                                   df2=ddf_integrated,
                                   string_column='matching_string',
                                   row_number_column='id',
                                   n_top=1500,
                                   threshold=0.8,
                                   n_gram=2,
                                   min_document_frequency=2,
                                   max_vocabulary_size=1500).select('i', 'j').sort('j', 'i').collect()
        assert len(res) == 0
