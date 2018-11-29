package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class ExactMatchIngestedWithDbConfig(
    integratedInputFile: String = "path-to-integrated-input-file",
    deltaInputFile: String = "path-to-delta-input-file",
    matchedExactOutputFile: String = "path-to-matched-exact-output-file",
    unmatchedIntegratedOutputFile: String = "path-to-unmatched-integrated-output-file",
    unmatchedDeltaOutputFile: String = "path-to-unmatched-delta-output-file"
) extends SparkJobConfig

object ContactPersonIntegratedExactMatch extends SparkJob[ExactMatchIngestedWithDbConfig] with GroupingFunctions {

  override private[spark] def defaultConfig = ExactMatchIngestedWithDbConfig()

  override private[spark] def configParser(): OptionParser[ExactMatchIngestedWithDbConfig] =
    new scopt.OptionParser[ExactMatchIngestedWithDbConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")
      opt[String]("integratedInputFile") required () action { (x, c) ⇒
        c.copy(integratedInputFile = x)
      } text "integratedInputFile is a string property"
      opt[String]("deltaInputFile") required () action { (x, c) ⇒
        c.copy(deltaInputFile = x)
      } text "deltaInputFile is a string property"
      opt[String]("matchedExactOutputFile") required () action { (x, c) ⇒
        c.copy(matchedExactOutputFile = x)
      } text "matchedExactOutputFile is a string property"
      opt[String]("unmatchedIntegratedOutputFile") required () action { (x, c) ⇒
        c.copy(unmatchedIntegratedOutputFile = x)
      } text "unmatchedIntegratedOutputFile is a string property"
      opt[String]("unmatchedDeltaOutputFile") required () action { (x, c) ⇒
        c.copy(unmatchedDeltaOutputFile = x)
      } text "unmatchedDeltaOutputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  def transform(spark: SparkSession, integratedContactPersons: Dataset[ContactPerson], dailyDeltaContactPersons: Dataset[ContactPerson]): (Dataset[ContactPerson], Dataset[ContactPerson], Dataset[ContactPerson]) = {
    import spark.implicits._

    val matchedExact: Dataset[ContactPerson] = determineExactMatches(spark, integratedContactPersons, dailyDeltaContactPersons)

    val unmatchedIntegrated = integratedContactPersons
      .filter('emailAddress.isNull && 'mobileNumber.isNull)
      .as[ContactPerson]

    val unmatchedDelta = dailyDeltaContactPersons
      .filter('emailAddress.isNull && 'mobileNumber.isNull)
      .as[ContactPerson]

    (matchedExact, unmatchedIntegrated, unmatchedDelta)
  }

  private def determineExactMatches(spark: SparkSession, integratedContactPersons: Dataset[ContactPerson], dailyDeltaContactPersons: Dataset[ContactPerson]): Dataset[ContactPerson] = {
    import spark.implicits._

    lazy val integratedWithExact = integratedContactPersons
      .filter('emailAddress.isNotNull || 'mobileNumber.isNotNull)
      .withColumn("group", concat(when('emailAddress.isNull, "").otherwise('emailAddress), when('mobileNumber.isNull, "").otherwise('mobileNumber)))
      .withColumn("inDelta", lit(false))

    lazy val newWithExact =
      dailyDeltaContactPersons
        .filter('emailAddress.isNotNull || 'mobileNumber.isNotNull)
        .withColumn("group", concat(when('emailAddress.isNull, "").otherwise('emailAddress), when('mobileNumber.isNull, "").otherwise('mobileNumber)))
        .withColumn("inDelta", lit(true))

    val w1 = Window.partitionBy($"group").orderBy($"ohubId".desc_nulls_last)

    val allExact = integratedWithExact
      .union(newWithExact)
      .withColumn("ohubId", first($"ohubId").over(w1)) // preserve ohubId
      .withColumn("ohubId", when('ohubId.isNull, createOhubIdUdf()).otherwise('ohubId))
      .withColumn("ohubId", first('ohubId).over(w1)) // make sure the whole group gets the same ohubId
      .drop("group")

    // NOTE: that allExact can contain exact matches from previous integrated which eventually need to be removed from the integrated result

    val w2 = Window.partitionBy($"concatId")
    allExact
      .withColumn("count", count("*").over(w2))
      .withColumn("select", when($"count" > 1, $"inDelta").otherwise(lit(true)))
      .filter($"select")
      .drop("count", "select", "inDelta")
      .as[ContactPerson]
  }

  override def run(spark: SparkSession, config: ExactMatchIngestedWithDbConfig, storage: Storage): Unit = {
    log.info(s"Integrated vs ingested exact matching contact persons from [${config.integratedInputFile}] and " +
      s"[${config.deltaInputFile}] to matched exact output [${config.matchedExactOutputFile}], unmatched integrated output to [${config.unmatchedIntegratedOutputFile}] and" +
      s"unmatched delta output [${config.unmatchedDeltaOutputFile}]")

    val integratedContactPersons = storage.readFromParquet[ContactPerson](config.integratedInputFile)
    val dailyDeltaContactPersons = storage.readFromParquet[ContactPerson](config.deltaInputFile)

    val (matchedExact, unmatchedIntegrated, unmatchedDelta) = transform(spark, integratedContactPersons, dailyDeltaContactPersons)

    storage.writeToParquet(matchedExact, config.matchedExactOutputFile)
    storage.writeToParquet(unmatchedIntegrated, config.unmatchedIntegratedOutputFile)
    storage.writeToParquet(unmatchedDelta, config.unmatchedDeltaOutputFile)
  }
}
