from string_matching import initial_load_matching as victim


class TestPreprocessingOperators(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo'),
                    ('2', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '5312BE'),
                    ('3', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB'),
                    ('4', 'DE', 'Bruce Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX'), ]

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('id', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                     'zipCode')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = victim.preprocess_operators(ddf).collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = victim.preprocess_operators(ddf).select('matching_string').collect()
        assert res[0][0] == 'dave mustaine amsterdam baravenue14b 5312be'


class TestPreprocessingContactPersons(object):

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo', 'foo', 'phone', 'email'),
                    ('2', 'NL', 'Dave', 'Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '5312BE', None, None),
                    ('3', 'NL', 'Ritchie', 'Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB', None, None),
                    ('4', 'DE', 'Bruce', 'Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX', None, None),
                    ('5', 'DE', None, None, 'bar', 'bar', '43', '1234AB', None, None),
                    ('6', 'DE', '', '', 'bar', 'bar', '43', '1234AB', None, None),
                    ('7', 'DE', 'firstName', 'emptyStreet', 'bar', '', None, '1234AB', None, None),
                    ]

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('id', 'countryCode', 'firstName', 'lastName',
                                                     'city', 'street', 'houseNumber',
                                                     'zipCode', 'mobilePhoneNumber', 'emailAddress')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = victim.preprocess_contacts(ddf).collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = victim.preprocess_contacts(ddf).select('matching_string').collect()
        assert res[0][0] == 'dave mustaine'


class TestOperatorMatching(object):
    @classmethod
    def setup_class(cls):
        def gen_tuple(tup, n):
            return [(str(n),) + tup[1:-1] + (tup[-1].format(idx=i),) for i in range(n)]

        cls.data = gen_tuple(
            ('generated_id', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '{idx}BE'),
            100)
        cls.data.extend([
            ('another_id', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB'),
        ])

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('id', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                     'zipCode')

    def test_full_matching(self, spark):
        ddf = self.create_ddf(spark)
        res = victim.apply_matching_on(ddf, spark,
                                       victim.preprocess_operators,
                                       victim.match_operators_for_country,
                                       'NL', 1500, 0.8).collect()
        assert len(res) == 99
        assert res[0][0] == 0
        assert res[0][1] == 1
