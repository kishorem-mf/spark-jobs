from typing import List
from time import perf_counter as timer

from pyspark.sql import functions as sf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

import findspark
findspark.init()


MINIMUM_ENTRIES_PER_COUNTRY = 100

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
    """
    # assigning at least 4GB to each core
    # NOTE: HDInsight by default allocates a single core per executor
    # regardless of what we set in here. The truth is in Yarn UI not in Spark UI.
    """
    spark = (SparkSession
             .builder
             .appName("NameMatching")
             .config('spark.dynamicAllocation.enabled', False)
             .config('spark.executorEnv.PYTHON_EGG_CACHE', '/tmp')
             .config('spark.executor.instances', 4)
             .config('spark.executor.cores', 13)
             .config('spark.executor.memory', '14g')
             .config('spark.driver.memory', '15g')
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("INFO")

    # the egg file should be in the same path as this script
    # EGG_NAME = 'string_matching.egg'
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    # sys.path.append(os.path.join(dir_path, 'dist', EGG_NAME))
    # sc.addPyFile(os.path.join(dir_path, 'dist', EGG_NAME))

    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().getLogger('org').setLevel(log4j.Level.WARN)
    log4j.LogManager.getRootLogger().getLogger('akka').setLevel(log4j.Level.ERROR)

    global LOGGER
    LOGGER = log4j.LogManager.getLogger(name)
    return spark


def read_parquet(spark: SparkSession, fn: str, fraction: float) -> DataFrame:
    return (spark
            .read
            .parquet(fn)
            .sample(False, fraction))


def get_country_codes(country_code_arg: str, ddf: DataFrame) -> List[str]:
    if country_code_arg == 'all':
        count_per_country = ddf.groupby('countryCode').count()
        LOGGER.info("Selecting countries with more than " + str(MINIMUM_ENTRIES_PER_COUNTRY) + " entries")
        codes = (
            count_per_country[count_per_country['count'] > MINIMUM_ENTRIES_PER_COUNTRY]
            .select('countryCode')
            .distinct()
            .rdd.map(lambda r: r[0]).collect())
    else:
        LOGGER.info("Selecting only country: " + country_code_arg)
        codes = [country_code_arg]
    return codes


def select_and_repartition_country(ddf: DataFrame, country_code: str) -> DataFrame:
    return (ddf
            .filter(sf.col('countryCode') == country_code)
            .drop('countryCode')
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
    grouping_window = (
        Window
        .partitionBy('j')
        .orderBy(sf.asc('i')))

    # keep only the first entry sorted alphabetically
    grp_sim = (
        ddf
        .withColumn("rn", sf.row_number().over(grouping_window))
        .filter(sf.col("rn") == 1)
        .drop('rn')
    )

    # remove group ID from column j
    return grp_sim.join(
        grp_sim.select('j').subtract(grp_sim.select('i')),
        on='j', how='inner'
    )


def save_to_parquet(ddf: DataFrame, fn):
    mode = 'append'
    LOGGER.info("Writing to: " + fn)
    LOGGER.info("Mode: " + mode)

    (ddf
        .coalesce(20)
        .write
        .partitionBy('countryCode')
        .parquet(fn, mode=mode))


def print_stats(ddf: DataFrame, n_top, threshold):
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


class Timer(object):
    def __init__(self, name: str, logger):
        self.logger = logger
        self.name = name
        self.start = timer()

    def end_and_log(self):
        end = timer()
        self.logger.info('{} took {} s'.format(self.name, str(end - self.start)))
