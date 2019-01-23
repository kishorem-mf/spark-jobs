import os
from typing import List

import pytest
from pyspark.sql.types import *
from string_matching.entity_matching import apply_operator_matching, apply_contactperson_matching, \
        preprocess_operators, preprocess_contact_persons


# pylint: disable=line-too-long
class TestPreprocessingOperators:

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, 'foo', 'foo', 'foo', 'foo'),
                    ('2', 'NL', 'Dave Mustaine ', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '5312BE'),
                    ('3', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet', '8', '1234AB'),
                    ('4', 'DE', 'Bruce Dickinson', 'Utrecht', 'Accacia Avenue', '22', '6666XX'), ]

    def create_ddf(self, spark):
        return spark.createDataFrame(self.data).toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber',    # noqa
                                                     'zipCode')

    def test_should_drop_null_names(self, spark):
        ddf = self.create_ddf(spark)
        res = preprocess_operators(ddf, 'concatId', True).collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = preprocess_operators(ddf, 'concatId', True).select('matching_string').collect()
        assert res[0][0] == 'dave mustaine amsterdam baravenue14b 5312be'


class TestPreprocessingContactPersons:

    @classmethod
    def setup_class(cls):
        cls.data = [('1', None, None, None, 'foo', 'foo', 'foo', 'foo', 'phone', 'email'),
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
        res = preprocess_contact_persons(ddf, 'concatId').collect()

        assert len(res) == 3
        assert ['2', '3', '4'] == [_[1] for _ in res]

    def test_match_string_should_be_concat_from_fields(self, spark):
        ddf = self.create_ddf(spark)

        res = preprocess_contact_persons(ddf, 'concatId').select('matching_string', 'streetCleansed').collect()
        assert res[0][0] == 'dave mustaine'
        assert res[0][1] == 'baravenue14b'


def gen_tuple(tup, n):
    return [(str(i),) + tup[1:-1] + (tup[-1].format(idx=i),) for i in range(n)]


@pytest.mark.skip()
class TestMatchingOperators:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city', 'street', 'houseNumber',
                                                'zipCode')

    def test_full_matching(self, spark):
        data = [
            # id country name            city           street        house zip
            ('1', 'NL', 'Dave Mustaine', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('2', 'NL', 'Dave Mustaine', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('3', 'NL', 'Dave Mustaien', 'Amsterdam  ', '@barAvenue', '\uFE3614b', '1234AB'),
            ('4', 'NL', 'Ritchie Blackmore', 'Utrecht', 'fooStreet',  '8',         '1234AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = apply_operator_matching(
            ddf,
            spark,
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
            data=[(str(i), "NL", name, "den haag", "stationsweg", "24", "3532AA") for i, name in enumerate(["benna", "benni", "benny", "benno", "benia", "alexander", "peter"])],    # noqa
            schema=StructType([StructField(field_name, StringType(), True) for field_name in ["concatId", "countryCode", "name", "city", "street", "houseNumber", "zipCode"]])    # noqa
        )

        result_df = apply_operator_matching(
            input_df, spark,
            threshold=0,
            min_norm_name_levenshtein_sim=0.7,
            return_levenshtein_similarity=True
        )

        result_df.show(truncate=False)
        # TODO create sensible test
        assert True

    @pytest.mark.parametrize("csv_name", ["identical_address_different_name.csv"])
    def test_full_matching_from_csv(self, spark, csv_name):
        csv_path = os.path.join(os.path.dirname(__file__), "data", "operators", csv_name)
        input_df = spark.read.format("csv").option("header", "true").load(csv_path)
        result_df = apply_operator_matching(
            input_df,
            spark,
            min_norm_name_levenshtein_sim=0,
            return_levenshtein_similarity=True
        )

        result_df.show(n=200, truncate=False)
        # TODO create sensible test
        assert True

    def test_validate_matching_operators(self, spark):
        ddf = spark.createDataFrame(
            data=[
                (0, "some_country", "Brown-Thornton", "Ebonybury", "Abbott Rue", "6", "07172"),
                (1, "some_country", "Washington Group", "Katelynville", "Debbie Ville", "8", "30336"),
                (2, "some_country", "Holloway LLC", "Dannyfort", "Mann Pines", None, "48296"),
                (3, "some_country", "Owen-Thomas", "Darinfurt", "Penny Plaza", None, "73947"),
                (4, "some_country", "Cobb-Nichols", "Darrellmouth", "Jeffery Extension", "4", "61626")
            ],
            schema=StructType(
                [
                    StructField("concatId", StringType(), True),
                    StructField("countryCode", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("street", StringType(), True),
                    StructField("houseNumber", StringType(), True),
                    StructField("zipCode", StringType(), True),
                ]
            ),
        )

        result_df = apply_operator_matching(ddf, spark, threshold=0)
        # With threshold 0, all matches should be returned, and we
        # expect n-1 pairs (e.g. records A,B,C,D -> pairs A,B & A,C & A,D).
        assert (ddf.count() - 1) == result_df.count()


class TestOhub1196:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city',
                                                'street', 'houseNumber', 'zipCode')
        # omitting region/state Victoria2/Alabama as name matching ignores them

    def test_full_matching(self, spark):
        data = [
            # id                  country name                city         street          house  zip
            ('AU~KANGAROO~E1-1401', 'AU', 'Pannenkoekenboot', 'Rotterdam', 'Weena',        '123', '1234 AB'),
            ('AU~KANGAROO~E1-1403', 'AU', 'test1',            'Melbourne', 'Main street2', '135', '3007'),
            ('AU~KANGAROO~E1-1235', 'AU', 'test1',            'Melbourne', 'Main street2', '135', '3007'),
            ('AU~KANGAROO~E1-1405', 'AU', 'test1',            'Melbourne', 'Main street2', '135', '3007'),
            ('AU~KANGAROO~E1-1404', 'AU', 'test1',            'Melbourne', 'Main street2', '135', '3007')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        print(len(res))
        print(res)
        assert len(res) == 3
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['AU~KANGAROO~E1-1235', 'AU~KANGAROO~E1-1235', 'AU~KANGAROO~E1-1235']
        assert targets == ['AU~KANGAROO~E1-1403', 'AU~KANGAROO~E1-1404', 'AU~KANGAROO~E1-1405']
        # failed to reproduce non-match


class TestNanne1:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city',
                                                'street', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', '', ''),
            # id country name           city   street        house zip
            ('5', 'NL', 'MC Donalds',   'Ede', 'Jansstraat', '8',  '2122AB'),
            ('6', 'NL', 'Donalds Farm', 'Ede', 'Damweg',     '12', '3333AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # failed to reproduce erroneous match


class TestNanne2:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city',
                                                'street', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', '', ''),
            # id country name           city         street        house zip
            ('5', 'NL', 'MC Donalds',   'Ede',       'Jansstraat', '8',  '2122AB'),
            ('6', 'NL', 'Donalds Farm', 'Hilversum', 'Damweg',     '12', '3333AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # no match as expected


class TestNanne3:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city',
                                                'street', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', '', ''),
            # id country name         city   street        house zip
            ('5', 'NL', 'MC Donalds', 'Ede', 'Jansstraat', '8',  '2122AB'),
            ('6', 'NL', 'MC Dondals', 'Ede', 'Damweg',     '12', '3333AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # failed to reproduce erroneous match


class TestNanne4:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city',
                                                'street', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', '', ''),
            # id country name         city          street        house zip
            ('5', 'NL', 'MC Donalds', 'Ede',        'Jansstraat', '8',  '2122AB'),
            ('6', 'NL', 'MC Dondals', 'Hilversum',  'Damweg',     '12', '3333AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # failed to reproduce erroneous match


class TestNanne5:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city',
                                                'street', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', '', ''),
            # id country name           city   street       house zip
            ('5', 'NL', 'MC Donalds',   'Ede', 'Jansstraat', '8', '2122AB'),
            ('6', 'NL', 'Donalds Farm', 'Ede', 'Damweg',    '12', '3333AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # no match (understandable)


class TestNanne6:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'name', 'city',
                                                'street', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', '', ''),
            # id country name                   city   street       house zip
            ('5', 'NL', 'Stichting aap',        'Ede', 'Jansstraat', '8', '2122AB'),
            ('6', 'NL', 'Stichting nijlpaard',  'Ede', 'Damweg',    '12', '3333AB')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # failed to reproduce erroneous match


# Operators data is not grouped correctly when Operators name and location has less similarity.

class TestOhub1188v1:
    def create_ddf(self, spark, data):
        # *1. Operator name and address details are different, but GR flag is set as False(Grouped to one Ohub Id) *
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'city',
                                                'street', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('BR', 'A', 'Z', '', '', '', ''),
            ('BR', 'BR~57', 'DALILA MARIA DE MATOS FREIRE', 'AVENIDA DESEMBARGADOR GONZAGA',
             'Fortaleza', '1316', '60823012'),
            ('BR', 'BR~70', 'R V MAGALHAES EIRELI', 'AVENIDA DESEMBARGADOR GONZAGA',
             'FORTALEZA', '1426', '60823012')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # failed to reproduce erroneous match


class TestOhub1188v2:
    def create_ddf(self, spark, data):
        # 2. Operator name is different and address details are same,
                # but GR flag is set as False(Grouped to one Ohub Id)
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            # country   concatId    name                              street          city         house   zip    # noqa
            ('DE', 'DE~DEX~10125123', 'BEHINDERTEN WERKSTÃ„TTE STIFT TILBEK', 'TILBECK', 'Havixbeck', '2', '48329'),  # noqa
            ('DE', 'DE~FUZZIT~4105C07D', 'Daniela Frerick Stift Tilbeck KVPH Gruppe', 'Tilbeck', 'Havixbeck', '2', '48329'),  # noqa
            ('DE', 'DE~FUZZIT~5D95D11D', 'Matthias BÃ¶yng Stift Tilbeck/ Ludgerusha', 'Tilbeck', 'Havixbeck', '2', '48329'),  # noqa
            ('DE', 'DE~FUZZIT~89D3D853', 'Monika Morbe Stift Tilbeck', 'Tilbeck', 'Havixbeck', '2', '48329'),
            ('DE', 'DE~DEX~501727', 'STIFT TILBECK', 'TILBECK', 'HAVIXBECK', '2', '48329'),
            ('DE', 'DE~FUZZIT~D058DF5D', 'Tilbecker WerkstÃ¤tten Stift Tilbeck GmbH', 'Tilbeck', 'Havixbeck', '2', '48329'),  # noqa
            ('DE', 'DE~FUZZIT~E8CA3616', 'Werner Gesmann Stift Tilbeck Zentral Ein', 'Tilbeck', 'Havixbeck', '2', '48329')  # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # failed to reproduce erroneous match


class TestOhub1188v3:
    def create_ddf(self, spark, data):
        # 3. Operator name is different, address details are not available,
                # but GR flag is set as False(Grouped to one Ohub Id)
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('BR', 'A', 'Z', '', '', '', ''),
            # country   concatId                name                                        street  city  house  zip
            ('BR', 'BR~IOPERA~17172258000178',  'CAMARGOS E CARDOSO PANIFICADORA LTDA ME',  '', '', '', ''),
            ('BR', 'BR~IOPERA~2220513000133',   'PANIFICADORA ALVES E CARDOSO LTDA ME',     '', '', '', '')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 0
        # failed to reproduce erroneous match


@pytest.mark.skip()
class TestOhub1188v4:
    def create_ddf(self, spark, data):
        # 4. Operator name is same and address details are different,
                # but GR flag is set as False(Grouped to one Ohub Id)
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA',           'LAKES BLVD',   'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '6972', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 1
        # match expected but not reproduced


class TestOprExactSame:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprMissingHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '',     '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprMissingZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprMissingCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', '', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprMissingStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', '',             'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprMissingName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', '',                               'BELLVILLE RD', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match as address by itself does not constitute sufficient evidence
        assert len(res) == 0


class TestOprNoHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '',     '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '',     '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprNoZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', ''),
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprNoCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', '',  '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', '',  '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprNoStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', '',    'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', '',    'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprNoName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', '',  'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', '',  'BELLVILLE RD', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match as name info is missing while address by itself does not constitute sufficient evidence
        assert len(res) == 0


class TestOprSlightlyDifferentHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1076', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprSlightlyDifferentZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31637')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprSlightlyDifferentCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARKS', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprSlightlyDifferentStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELVILLE RD', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprSlightlyDifferentName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 5 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprVeryDifferentHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '4321', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


class TestOprVeryDifferentZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '98765')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


@pytest.mark.skip()
class TestOprVeryDifferentCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'NEW YORK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


class TestOprVeryDifferentStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'ANOTHER STREET', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


class TestOprVeryDifferentName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'KENTUCKY FRIED CHICKEN',         'BELLVILLE RD', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


class TestOprSuperfluousHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075-6', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprSuperfluousZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '031636')
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprSuperfluousCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK CA', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprSuperfluousStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street                  city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD',         'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE ROAD NORTH', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprSuperfluousName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6',                        'BELLVILLE RD', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprMultipleSuperfluous:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street                  city         house     zip
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE ROAD NORTH', 'LAKE PARK', '1075-6', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6',                        'BELLVILLE RD',         'LAKE PARK', '1075',   '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprDifferentSuperfluous:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street                  city         house     zip
            ('US', 'US~ANTHEM~636', 'MOTEL 6',                        'BELLVILLE ROAD NORTH', 'LAKE PARK', '1075-6', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD',         'LAKE PARK', '1075',   '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprMultipleMissing:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', '',             'LAKE PARK', '',     '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprDifferentMissingLongName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', '',             'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '',     '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprDifferentMissingShortName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name       street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6', '',             'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6', 'BELLVILLE RD', 'LAKE PARK', '',     '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprMultipleSlightlyDifferent:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6 LAKE PARK', 'BELVILLE RD', 'LAKE PARK', '1075-6', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestOprMultipleVeryDifferent:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'KENTUCKY FRIED CHICKEN',         'ANOTHER ST', 'LAKE PARK', '1234', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


@pytest.mark.skip()
class TestOprMixedDiscrepancies:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'MOTEL 6',                        '',             'LAKE PARK', '1075', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestOprSubtlyDistinct:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('countryCode', 'concatId', 'name', 'street',
                                                'city', 'houseNumber', 'zipCode')

    def test_full_matching(self, spark):
        data = [
            ('US', 'A', 'Z', '', '', '', ''),
            # country   concatId    name                              street          city         house   zip    # noqa
            ('US', 'US~ANTHEM~636', 'MOTEL 6 LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '1075', '31636'),    # noqa
            ('US', 'US~ANTHEM~637', 'PARK HOTEL LAKE PARK GA LAKE PARK', 'BELLVILLE RD', 'LAKE PARK', '103', '31636')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_operator_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


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
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b'),   # noqa
            ('3', 'NL', 'Dave', 'Mustaire', 'Amsterdam  ', '@barAvenue', None, None, '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        assert len(res) == 1
        sources = [_[0] for _ in res]
        targets = [_[1] for _ in res]
        assert sources == ['1']
        assert targets == ['2']


class TestCpnExactSame:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMissingFirstName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', '',     'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMissingLastName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', '',         'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMissingCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', '',            '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMissingStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '',           '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnMissingMobile:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '',            'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnMissingEmail:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', '',                         '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMissingZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '',       '14b')   # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnMissingHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '')   # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnNoFirstName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', '',     'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', '',     'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnNoLastName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', '',         'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', '',         'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnNoCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city        street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', '',         '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', '',         '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnNoStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street   mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '',      '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '',      '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnNoMobile:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '',            'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '',            'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnNoEmail:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress    zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', '',             '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', '',             '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnNoZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '',       '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '',       '14b')   # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnNoHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', ''),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnSlightlyDifferentFirstName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave',  'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'David', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnSlightlyDifferentLastName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustane',  'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSlightlyDifferentCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amstardam', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSlightlyDifferentStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', 'bar avenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSlightlyDifferentMobile:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '061234567',   'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSlightlyDifferentEmail:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.there@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnSlightlyDifferentZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314DE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSlightlyDifferentHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '15b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnVeryDifferentFirstName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Rose', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


class TestCpnVeryDifferentLastName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Brouwer',   'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


class TestCpnVeryDifferentCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'London',      '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnVeryDifferentStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street          mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue',   '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', 'Keizerstraat', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnVeryDifferentMobile:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3129876543', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnVeryDifferentEmail:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'email@example.org',        '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnVeryDifferentZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '9876ZY', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnVeryDifferentHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '90A')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnSuperfluousFirstName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first        lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave',        'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Davidissimo', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnSuperfluousLastName:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName          city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine',       'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine-Wells', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSuperfluousCity:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city             street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ',   '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'New Amsterdam', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnSuperfluousStreet:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street              mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue',       '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue north', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSuperfluousMobile:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '061234567',   'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSuperfluousEmail:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                        zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com',         '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here+comment@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnSuperfluousZipCode:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314',   '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSuperfluousHouseNumber:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14'),    # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14-5b')  # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMultipleSuperfluous:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first  lastName          city         street        mobile         emailAddress                        zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave',  'Mustaine',       "A'dam",     '@barAvenue', '061234567',   'spam.here@mailinator.com',         '5314',   '14'),       # noqa
            ('2', 'NL', 'David', 'Mustaine-Wells', 'Amsterdam', '@barAvenue', '+3161234567', 'spam.here+comment@mailinator.com', '5314BE', '14-15b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnDifferentSuperfluous:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first  lastName          city         street        mobile         emailAddress                        zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave',  'Mustaine-Wells', "A'dam",     '@barAvenue', '+3161234567', 'spam.here@mailinator.com',         '5314BE', '14'),    # noqa
            ('2', 'NL', 'David', 'Mustaine',       'Amsterdam', '@barAvenue', '061234567',   'spam.here+comment@mailinator.com', '5314',   '14-15b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMultipleMissing:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '',           '',            '',                         '',       '')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnDifferentMissing:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', '',            '@barAvenue', '+3161234567', '',                         '5314BE', ''),    # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '',           '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMultipleSlightlyDifferent:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam',   '@barAvenue', '061234567',   'spam.here@mailinator.com', '5314BE', '14'),    # noqa
            ('2', 'NL', 'Dave', 'Mustane', 'Amsterdam',    '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


@pytest.mark.skip()
class TestCpnMultipleVeryDifferentMoved:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city         street           mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam', '@barAvenue',    '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Mustaine', 'Leiden',    'Stationstraat', '+3161234567', 'work@email.com',           '1234AB', '123')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match, name + e.g. mobile here will to a reasonable person constitute sufficient evidence
        assert len(res) == 1


class TestCpnMultipleVeryDifferentWrong:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam  ', '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Rose', 'Wells',    'Amsterdam  ', '@barAvenue', '+3169876543', 'spam.here@mailinator.com', '5314BE', '14b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0


@pytest.mark.skip()
class TestCpnMixedDiscrepancies:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city  street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine-Wells', 'Amsterdam  ', '@barAvenue', '+3161234567', '',                         '5314BE', '14'),    # noqa
            ('2', 'NL', 'Dave', 'Mustaine',       'Amsterdam  ', '@barAvenue', '+3169876543', 'spam.here@mailinator.com', '',       '14-15b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect match
        assert len(res) == 1


class TestCpnSubtlyDistinct:
    def create_ddf(self, spark, data):
        return spark.createDataFrame(data).toDF('concatId', 'countryCode', 'firstName', 'lastName', 'city', 'street',
                                                'mobileNumber', 'emailAddress', 'zipCode', 'houseNumber')

    def test_full_matching(self, spark):
        data = [
            ('0', 'NL', 'Z', '', '', 'Z', None, None, '', ''),
            # id  country first lastName    city           street        mobile         emailAddress                zipCode   houseNumber    # noqa
            ('1', 'NL', 'Dave', 'Mustaine', 'Amsterdam',   '@barAvenue', '+3161234567', 'spam.here@mailinator.com', '5314BE', '14b'),   # noqa
            ('2', 'NL', 'Dave', 'Dustaine', 'Rotterdam',   '@barAvenue', '+3161234560', 'spam.here@mailinator.org', '6314BE', '24b')    # noqa
        ]
        ddf = self.create_ddf(spark, data)
        res = (apply_contactperson_matching(ddf, spark)
               .select('sourceId', 'targetId').sort('targetId').collect())
        # expect no match
        assert len(res) == 0

# pylint: enable=line-too-long
