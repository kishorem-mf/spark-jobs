package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import scopt.OptionParser

import scala.reflect.runtime.universe._

case class ExactAndFuzzyMatchesConfig(
    exactMatchedInputFile: String = "exact-matched-input-file",
    fuzzyMatchedDeltaIntegratedInputFile: String = "fuzzy-matched-delta-integrated-input-file",
    deltaGoldenRecordsInputFile: String = "delta-golden-records-input-file",
    combinedOutputFile: String = "combined-output-file"
) extends SparkJobConfig

abstract class BaseCombining[T <: DomainEntity: TypeTag] extends SparkJob[ExactAndFuzzyMatchesConfig] {

  override private[spark] def defaultConfig = ExactAndFuzzyMatchesConfig()

  override private[spark] def configParser(): OptionParser[ExactAndFuzzyMatchesConfig] =
    new scopt.OptionParser[ExactAndFuzzyMatchesConfig]("Exact and fuzzy matches combiner") {
      head("combines dataset from exact matches and fuzzy matches", "1.0")
      opt[String]("exactMatchedInputFile") required () action { (x, c) ⇒
        c.copy(exactMatchedInputFile = x)
      } text "exactMatchedInputFile is a string property"
      opt[String]("fuzzyMatchedDeltaIntegratedInputFile") required () action { (x, c) ⇒
        c.copy(fuzzyMatchedDeltaIntegratedInputFile = x)
      } text "fuzzyMatchedDeltaIntegratedInputFile is a string property"
      opt[String]("deltaGoldenRecordsInputFile") required () action { (x, c) ⇒
        c.copy(deltaGoldenRecordsInputFile = x)
      } text "deltaGoldenRecordsInputFile is a string property"
      opt[String]("combinedOutputFile") required () action { (x, c) ⇒
        c.copy(combinedOutputFile = x)
      } text "combinedOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def transform(
   spark: SparkSession,
   exactMatches: Dataset[T],
   fuzzyMatchesDeltaIntegrated: Dataset[T],
   deltaGoldenRecords: Dataset[T]
  ): Dataset[T] = {
    import spark.implicits._

    // deduplicate records by selecting the 'newest' one (based on ohubCreated) per unique concatId.
    val w = Window.partitionBy('concatId).orderBy('ohubCreated.desc)

    val all = exactMatches
      .unionByName(fuzzyMatchesDeltaIntegrated)
      .unionByName(deltaGoldenRecords)

    all
      .withColumn("rn", row_number.over(w))
      .filter('rn === 1)
      .drop('rn)
      .as[T]
  }

  override def run(spark: SparkSession, config: ExactAndFuzzyMatchesConfig, storage: Storage): Unit = {
    log.info(
      s"""Combining exact match results from [${config.exactMatchedInputFile}]
         |with fuzzy match results from [${config.fuzzyMatchedDeltaIntegratedInputFile}]
         |and [${config.deltaGoldenRecordsInputFile}]
         |and write results to [${config.combinedOutputFile}]""".stripMargin)

    val exactMatches = storage.readFromParquet[T](config.exactMatchedInputFile)
    val fuzzyMatchesDeltaIntegrated = storage.readFromParquet[T](config.fuzzyMatchedDeltaIntegratedInputFile)
    val deltaGoldenRecords = storage.readFromParquet[T](config.deltaGoldenRecordsInputFile)

    val result: Dataset[T] = transform(spark, exactMatches, fuzzyMatchesDeltaIntegrated, deltaGoldenRecords)

    storage.writeToParquet(result, config.combinedOutputFile)
  }
}
