package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.merging.DataFrameHelpers._
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scopt.OptionParser

object ContactPersonIntegratedExactMatch extends SparkJob[ExactMatchIngestedWithDbConfig] {

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

  def transform(
    integratedContactPersons: Dataset[ContactPerson],
    dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): (Dataset[ContactPerson], Dataset[ContactPerson], Dataset[ContactPerson]) = {
    import spark.implicits._

    val matchedExact: Dataset[ContactPerson] = determineExactMatches(integratedContactPersons, dailyDeltaContactPersons)

    val unmatchedIntegrated = integratedContactPersons
      .filter('emailAddress.isNull && 'mobileNumber.isNull)
      .as[ContactPerson]

    val unmatchedDelta = dailyDeltaContactPersons
      .filter('emailAddress.isNull && 'mobileNumber.isNull)
      .as[ContactPerson]

    (matchedExact, unmatchedIntegrated, unmatchedDelta)
  }

  private def determineExactMatches(
    integratedContactPersons: Dataset[ContactPerson],
    dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): Dataset[ContactPerson] = {
    import spark.implicits._

    val mobileAndEmail = Seq("emailAddress", "mobileNumber").map(col)

    lazy val integratedWithExact = integratedContactPersons
      .toDF
      .filter('emailAddress.isNotNull || 'mobileNumber.isNotNull)
      .concatenateColumns("group", mobileAndEmail)
      .withColumn("inDelta", lit(false))

    lazy val newWithExact =
      dailyDeltaContactPersons
        .toDF
        .filter('emailAddress.isNotNull || 'mobileNumber.isNotNull)
        .concatenateColumns("group", mobileAndEmail)
        .withColumn("inDelta", lit(true))

    val allExact = integratedWithExact
      .union(newWithExact)
      .addOhubId
      .drop("group")

    // NOTE: that allExact can contain exact matches from previous integrated which eventually need to be removed from the integrated result

    val w2 = Window.partitionBy($"concatId")
    allExact
      .selectLatestRecord
      .drop("inDelta")
      .as[ContactPerson]
  }

  override def run(spark: SparkSession, config: ExactMatchIngestedWithDbConfig, storage: Storage): Unit = {
    log.info(
      s"""
         |Integrated vs ingested exact matching contact persons from [${config.integratedInputFile}] and  [${config.deltaInputFile}]
         |to matched exact output [${config.matchedExactOutputFile}],
         |unmatched integrated output to [${config.unmatchedIntegratedOutputFile}]
         |and unmatched delta output [${config.unmatchedDeltaOutputFile}]""".stripMargin)

    implicit val implicitSpark: SparkSession = spark
    val integratedContactPersons = storage.readFromParquet[ContactPerson](config.integratedInputFile)
    val dailyDeltaContactPersons = storage.readFromParquet[ContactPerson](config.deltaInputFile)

    val (matchedExact, unmatchedIntegrated, unmatchedDelta) = transform(integratedContactPersons, dailyDeltaContactPersons)

    storage.writeToParquet(matchedExact, config.matchedExactOutputFile)
    storage.writeToParquet(unmatchedIntegrated, config.unmatchedIntegratedOutputFile)
    storage.writeToParquet(unmatchedDelta, config.unmatchedDeltaOutputFile)
  }
}
