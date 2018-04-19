import match_operators


class TestPreprocessing(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo'),
                    ('2', 'NL', 'Dave Mustaine', 'Amsterdam', 'barAvenue', '+3106918503', '5312BE'),
                    ('3', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '+3102345678', '1234AB'),
                    ('4', 'DE', 'Paul Gilbert', 'Utrecht', 'fooStreet', '+3102345678', '1234AB'), ]

    def test_should_drop_null_names(self, spark):
        ddf = spark.createDataFrame(self.data).toDF('id', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                    'zipCode')

        res = match_operators.preprocess_operators(ddf).collect()
        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]
