import pytest
from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestAnswers(object):

    @pytest.mark.skip(reason="not yet implemented")
    def test_full_matching_answers(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/answers", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/answers.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/answers_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/answers", 1)
