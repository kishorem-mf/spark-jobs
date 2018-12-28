from pyspark.sql.types import *
from test_utils import assertDataframeCount

class TestQuestions(object):

    def test_full_matching_questions(self, spark):
        # raw contains 1 records (todo add proper test data)...input integrated is empty

        assertDataframeCount(spark, "/usr/local/data/input/integrated/questions", 0)

        assertDataframeCount(spark, "/usr/local/data/ingested/common/questions.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/intermediate/questions_pre_processed.parquet", 1)

        assertDataframeCount(spark, "/usr/local/data/output/integrated/questions", 1)
