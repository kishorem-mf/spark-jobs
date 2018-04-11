import re
from time import perf_counter as timer
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.window import Window

MINIMUM_ENTRIES_PER_COUNTRY = 100
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


def start_spark(name):
    spark = (SparkSession
             .builder
             .appName("NameMatching")
             # .config('spark.dynamicAllocation.enabled', False)
             # .config('spark.executor.instances', 4)
             # .config('spark.executor.cores', 13)
             # .config('spark.executor.memory', '14g')
             # .config('spark.driver.memory', '15g')
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("INFO")

    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().getLogger('org').setLevel(log4j.Level.WARN)
    log4j.LogManager.getRootLogger().getLogger('akka').setLevel(log4j.Level.ERROR)
    global LOGGER
    LOGGER = log4j.LogManager.getLogger(name)
    return spark, LOGGER


def read_parquet(spark: SparkSession, fn: str, fraction: float) -> DataFrame:
    return (spark
            .read
            .parquet(fn)
            .sample(False, fraction))


def get_country_codes(country_code_arg: str, ddf: DataFrame, minimum_entries=MINIMUM_ENTRIES_PER_COUNTRY) -> List[str]:
    count_per_country = ddf.groupby('countryCode').count()
    LOGGER.info("Selecting countries with more than " + str(minimum_entries) + " entries")
    codes = sorted(count_per_country[count_per_country['count'] > minimum_entries]
                   .select('countryCode')
                   .distinct()
                   .rdd.map(lambda r: r[0]).collect())
    if country_code_arg != 'all':
        LOGGER.info("Selecting only union of all countries (with MINIMUM_ENTRIES_PER_COUNTRY) and: " + country_code_arg)
        codes = set(codes) & set([country_code_arg])
    LOGGER.info("Selected countries are: {}".format(codes))
    return list(codes)


def select_and_repartition_country(ddf: DataFrame, column_name: str, country_code: str) -> DataFrame:
    return (ddf
            .filter(sf.col(column_name) == country_code)
            .drop(column_name)
            .repartition('id')
            .sort('id', ascending=True))


def group_matches(ddf: DataFrame) -> DataFrame:
    """
    To generate a final list of matches, in the form of (i, j), i.e. contact i matches with contact j,
    we do the following:
    - make sure each j only matches with one i (the 'group leader')
        - note: of course we can have multiple matches per group leader, e.g. (i, j) and (i, k)
    - make sure that each i (group leader) is not matched with another 'group leader',
      e.g. if we have (i, j) we remove (k, i) for all k
    """
    grouping_window = (Window
                       .partitionBy('j')
                       .orderBy(sf.asc('i')))

    # keep only the first entry sorted alphabetically
    grp_sim = (ddf
               .withColumn("rn", sf.row_number().over(grouping_window))
               .filter(sf.col("rn") == 1)
               .drop('rn')
               )

    # remove group ID from column j
    return grp_sim.join(
        grp_sim.select('j').subtract(grp_sim.select('i')),
        on='j', how='inner'
    )


def save_to_parquet(ddf: DataFrame, fn: str, mode: str):
    LOGGER.info("Writing to: " + fn)
    LOGGER.info("Mode: " + mode)
    (ddf
     .coalesce(20)
     .write
     .partitionBy('countryCode')
     .parquet(fn, mode=mode)
     )


def print_stats_operators(ddf: DataFrame, n_top, threshold):
    ddf.persist()
    n_matches = ddf.count()

    print('\n\nNr. Similarities:\t', n_matches)
    print('Threshold:\t', threshold)
    print('N_top:\t', n_top)
    (ddf
     .select('sourceId', 'targetId',
             'similarity', 'sourceName', 'targetName')
     .sort('similarity', ascending=True)
     .show(50, truncate=False))

    (ddf
     .groupBy(['sourceId', 'sourceName'])
     .count()
     .sort('count', ascending=False).show(50, truncate=False))

    ddf.describe('similarity').show()
    ddf.unpersist()


def print_stats_contacts(ddf: DataFrame, n_top, threshold):
    ddf.persist()
    n_matches = ddf.count()

    print('\n\nNr. Similarities:\t', n_matches)
    print('Threshold:\t', threshold)
    print('N_top:\t', n_top)
    (ddf
     .select('SIMILARITY',
             'SOURCE_NAME', 'TARGET_NAME',
             'SOURCE_STREET', 'TARGET_STREET',
             'SOURCE_ZIP_CODE', 'TARGET_ZIP_CODE',
             'SOURCE_CITY', 'TARGET_CITY')
     .sort('SIMILARITY', ascending=True)
     .show(50, truncate=False))

    (ddf
     .groupBy(['SOURCE_ID', 'SOURCE_NAME'])
     .count()
     .sort('count', ascending=False).show(50, truncate=False))

    ddf.describe('SIMILARITY').show()
    ddf.unpersist()


class Timer(object):
    def __init__(self, name: str, logger):
        self.logger = logger
        self.name = name
        self.start = timer()

    def end_and_log(self):
        end = timer()
        self.logger.info('{} took {} s'.format(self.name, str(end - self.start)))


def remove_strange_chars_to_lower_and_trim(input: str):
    if input is None:
        return input
    p = re.compile(
        "(^\\s+)|(\\s+$)|[\u0024\u00A2\u00A3\u00A4\u00A5\u058F\u060B\u09F2\u09F3\u09FB\u0AF1\u0BF9\u0E3F\u17DB\u20A0"
        "\u20A1\u20A2\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2"
        "\u20B3\u20B4\u20B5\u20B6\u20B7\u20B8\u20B9\u20BA\u20BB\u20BC\u20BD\u20BE\uA838\uFDFC\uFE69\uFF04\uFFE0\uFFE1"
        "\uFFE5\uFFE6\u0081°”\\\\_\\'\\~`!@#%()={}|:;\\?/<>,\\.\\[\\]\\+\\-\\*\\^&:]+")

    input = p.sub('', input.lower())
    return input.strip()


def remove_spaces_strange_chars_and_to_lower(input: str):
    if input is None:
        return input
    p = re.compile(
        "[ \u0024\u00A2\u00A3\u00A4\u00A5\u058F\u060B\u09F2\u09F3\u09FB\u0AF1\u0BF9\u0E3F\u17DB\u20A0\u20A1\u20A2"
        "\u20A3\u20A4\u20A5\u20A6\u20A7\u20A8\u20A9\u20AA\u20AB\u20AC\u20AD\u20AE\u20AF\u20B0\u20B1\u20B2\u20B3\u20B4"
        "\u20B5\u20B6\u20B7\u20B8\u20B9\u20BA\u20BB\u20BC\u20BD\u20BE\uA838\uFDFC\uFE69\uFF04\uFFE0\uFFE1\uFFE5\uFFE6"
        "\u0081°”\\\\_\\'\\~`!@#$%()={}|:;\\?/<>,\\.\\[\\]\\+\\-\\*\\^&:]+")

    input = p.sub('', input.lower())
    return input
