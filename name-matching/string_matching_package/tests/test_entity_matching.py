import os
from typing import List

import pytest
from pyspark.sql.types import *
from string_matching import entity_matching


class TestPreprocessingOperators:

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo'),
                    ('2', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '5312BE'),
                    ('3', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB'),
                    ('4', 'DE', 'Bruce Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX'), ]

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                     'zipCode')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = entity_matching.preprocess_operators(ddf, 'concatId', True).collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = entity_matching.preprocess_operators(ddf, 'concatId', True).select('matching_string').collect()
        assert res[0][0] == 'dave mustaine amsterdam baravenue14b 5312be'


class TestPreprocessingContactPersons:

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo', 'foo', 'phone', 'email'),
                    ('2', 'NL', 'Dave', 'Mustaine ', 'Amsterdam  ', '@barAvenue', '\u09F214b', '5312BE', None, None),
                    ('3', 'NL', 'Ritchie', 'Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB', None, None),
                    ('4', 'DE', 'Bruce', 'Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX', None, None),
                    ('5', 'DE', None, None, 'bar', 'bar', '43', '1234AB', None, None),
                    ('6', 'DE', '', '', 'bar', 'bar', '43', '1234AB', None, None),
                    ('7', 'DE', 'firstName', 'emptyStreet', 'bar', '', None, '1234AB', None, None),
                    ]

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('concatId', 'countryCode', 'firstName', 'lastName',
                                                     'city', 'street', 'houseNumber',
                                                     'zipCode', 'mobileNumber', 'emailAddress')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = entity_matching.preprocess_contact_persons(ddf, 'concatId').collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = entity_matching.preprocess_contact_persons(ddf, 'concatId').select('matching_string', 'streetCleansed').collect()
        assert res[0][0] == 'dave mustaine'
        assert res[0][1] == 'baravenue14b'


def gen_tuple(tup, n):
    return [(str(i),) + tup[1:-1] + (tup[-1].format(idx=i),) for i in range(n)]


class TestMatchingOperators:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('1', 'NL', 'Dave Mustaine', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('2', 'NL', 'Dave Mustaine', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('3', 'NL', 'Dave Mustaien', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('4', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = entity_matching.apply_operator_matching(
            ddf,
            spark,
            entity_matching.preprocess_operators,
            1500,
            0.8,
            min_norm_name_levenshtein_sim=0
        ).select('sourceId', 'targetId').sort('targetId').collect()
        assert len(res) == 2
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['1', '1']
        assert targets == ['2', '3']

    def test_matching_levenshtein_threshold(self, spark):
        # Generate a DataFrame with identical address and slightly different names.
        input_df = spark.createDataFrame(
            data=[(str(i),"NL",name,"den haag","stationsweg","24","3532AA") for i, name in enumerate(["benna", "benni", "benny", "benno", "benia", "alexander", "peter"])],
            schema=StructType([StructField(field_name, StringType(), True) for field_name in ["concatId","countryCode","name","city","street","houseNumber","zipCode"]])
        )

        result_df = entity_matching.apply_operator_matching(
            records_per_country=input_df,
            spark=spark,
            preprocess_function=entity_matching.preprocess_operators,
            n_top=1500,
            threshold=0,
            min_norm_name_levenshtein_sim=0.7,
            postprocess_return_levenshtein_similarity=True
        )

        result_df.show(truncate=False)
        # TODO create sensible test
        assert True

    @pytest.mark.parametrize("csv_name", ["identical_address_different_name.csv"])
    def test_full_matching_from_csv(self, spark, csv_name):
        csv_path = os.path.join(os.path.dirname(__file__), "data", "operators", csv_name)
        input_df = spark.read.format("csv").option("header", "true").load(csv_path)
        result_df = entity_matching.apply_operator_matching(
            records_per_country=input_df,
            spark=spark,
            preprocess_function=entity_matching.preprocess_operators,
            n_top=1500,
            threshold=0.8,
            min_norm_name_levenshtein_sim=0,
            postprocess_return_levenshtein_similarity=True
        )

        result_df.show(n=200, truncate=False)
        # TODO create sensible test
        assert True


class TestMatchingContactPersons:

    def create_ddf(self, spark, data: List[tuple]):
        schema = StructType([
            StructField("concatId", StringType(), True),
            StructField("countryCode", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("city", StringType(), True),
            StructField("street", StringType(), True),
            StructField("mobileNumber", StringType(), True),
            StructField("emailAddress", StringType(), True),
            StructField("zipCode", StringType(), True),
            StructField("houseNumber", StringType(), True),
        ])
        return spark.createDataFrame(data, schema)

    def test_full_matching(self, spark):
        data = [
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b'),
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b'),
            ('3', 'NL', 'Dave', 'Mustaire', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b')
        ]
        ddf = self.create_ddf(spark, data)
        res = entity_matching.apply_matching_on(
            ddf,
            spark,
            entity_matching.preprocess_contact_persons,
            entity_matching.postprocess_contact_persons,
            1500,
            0.8
        ).select('sourceId', 'targetId').sort('targetId').collect()
        assert len(res) == 1
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['1']
        assert targets == ['2']
