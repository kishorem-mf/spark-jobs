package com.unilever.ohub.spark.combining

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ExactAndFuzzyMatchesConfig(
    contactPersonExactMatchedInputFile: String = "contact-person-exact-matched-input-file",
    contactPersonFuzzyMatchedDeltaIntegratedInputFile: String = "contact-person-fuzzy-matched-delta-integrated-input-file",
    contactPersonsDeltaGoldenRecordsInputFile: String = "contact-persons-delta-golden-records-input-file",
    contactPersonsCombinedOutputFile: String = "contact-persons-combined-output-file"
) extends SparkJobConfig

object ContactPersonCombineExactAndFuzzyMatches extends SparkJob[ExactAndFuzzyMatchesConfig] {

  override private[spark] def defaultConfig = ExactAndFuzzyMatchesConfig()

  override private[spark] def configParser(): OptionParser[ExactAndFuzzyMatchesConfig] =
    new scopt.OptionParser[ExactAndFuzzyMatchesConfig]("Contact person exact and fuzzy matches combiner") {
      head("combines contact persons from exact matches and fuzzy matches", "1.0")
      opt[String]("contactPersonExactMatchedInputFile") required () action { (x, c) ⇒
        c.copy(contactPersonExactMatchedInputFile = x)
      } text "contactPersonExactMatchedInputFile is a string property"
      opt[String]("contactPersonFuzzyMatchedDeltaIntegratedInputFile") required () action { (x, c) ⇒
        c.copy(contactPersonFuzzyMatchedDeltaIntegratedInputFile = x)
      } text "contactPersonFuzzyMatchedDeltaIntegratedInputFile is a string property"
      opt[String]("contactPersonsDeltaGoldenRecordsInputFile") required () action { (x, c) ⇒
        c.copy(contactPersonsDeltaGoldenRecordsInputFile = x)
      } text "contactPersonsDeltaGoldenRecordsInputFile is a string property"
      opt[String]("contactPersonsCombinedOutputFile") required () action { (x, c) ⇒
        c.copy(contactPersonsCombinedOutputFile = x)
      } text "contactPersonsCombinedOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def transform(
    spark: SparkSession,
    contactPersonExactMatches: Dataset[ContactPerson],
    contactPersonFuzzyMatchesDeltaIntegrated: Dataset[ContactPerson],
    contactPersonFuzzyMatchesDeltaLeftOvers: Dataset[ContactPerson]
  ): Dataset[ContactPerson] = {
    import spark.implicits._

    // deduplicate contact persons by selecting the 'newest' one (based on ohubCreated) per unique concatId.
    val w = Window.partitionBy('concatId).orderBy('ohubCreated.desc)

    contactPersonExactMatches
      .unionByName(contactPersonFuzzyMatchesDeltaIntegrated)
      .unionByName(contactPersonFuzzyMatchesDeltaLeftOvers)
      .withColumn("rn", row_number.over(w))
      .filter('rn === 1)
      .drop('rn)
      .as[ContactPerson]
  }

  override def run(spark: SparkSession, config: ExactAndFuzzyMatchesConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Combining contact person exact match results from [${config.contactPersonExactMatchedInputFile}] with fuzzy match results from " +
      s"[${config.contactPersonFuzzyMatchedDeltaIntegratedInputFile}] and [${config.contactPersonsDeltaGoldenRecordsInputFile}] and write results to [${config.contactPersonsCombinedOutputFile}]")

    val contactPersonExactMatches = storage.readFromParquet[ContactPerson](config.contactPersonExactMatchedInputFile)
    val contactPersonFuzzyMatchesDeltaIntegrated = storage.readFromParquet[ContactPerson](config.contactPersonFuzzyMatchedDeltaIntegratedInputFile)
    val contactPersonFuzzyMatchesDeltaLeftOvers = storage.readFromParquet[ContactPerson](config.contactPersonsDeltaGoldenRecordsInputFile)

    val result: Dataset[ContactPerson] = transform(spark, contactPersonExactMatches, contactPersonFuzzyMatchesDeltaIntegrated, contactPersonFuzzyMatchesDeltaLeftOvers)

    storage.writeToParquet(result, config.contactPersonsCombinedOutputFile)
  }
}
