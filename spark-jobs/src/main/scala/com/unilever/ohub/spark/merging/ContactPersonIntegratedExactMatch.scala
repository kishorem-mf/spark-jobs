package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.merging.DataFrameHelpers._
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import scopt.OptionParser

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

  def transform(
    integratedContactPersons: Dataset[ContactPerson],
    dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): (Dataset[ContactPerson], Dataset[ContactPerson], Dataset[ContactPerson]) = {
    import spark.implicits._

    val matchedExactEmail: Dataset[ContactPerson] = determineExactMatchByEmail(
      integratedContactPersons,
      dailyDeltaContactPersons)

    val matchedExactPhone: Dataset[ContactPerson] = determineExactMatchByPhone(
      integratedContactPersons,
      dailyDeltaContactPersons)

    val matchedExactAll = matchedExactEmail
                              .union(matchedExactPhone)

    val unmatchedIntegrated = integratedContactPersons
      .join(matchedExactAll, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    val unmatchedDelta = dailyDeltaContactPersons
      .join(matchedExactAll, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    (matchedExactAll, unmatchedIntegrated, unmatchedDelta)
  }


  private def determineExactMatchByEmail(
                                          integratedContactPersons: Dataset[ContactPerson],
                                          dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): Dataset[ContactPerson] = {
    import spark.implicits._

    val byEmail = Seq("emailAddress").map(col)
    val emailNotNull = $"emailAddress".isNotNull

    lazy val integratedWithExact = integratedContactPersons
      .filter(emailNotNull)
      .concatenateColumns("group", byEmail)
      .withColumn("inDelta", lit(false))

    lazy val newWithExact =
      dailyDeltaContactPersons
        .filter(emailNotNull)
        .concatenateColumns("group", byEmail)
        .withColumn("inDelta", lit(true))

    integratedWithExact
      .union(newWithExact)
      .addOhubId
      .drop("group")
      .selectLatestRecord
      .drop("inDelta")
      //      .removeSingletonGroups
      .as[ContactPerson]
  }
  private def determineExactMatchByPhone(integratedContactPersons: Dataset[ContactPerson],
                                         dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): Dataset[ContactPerson] = {
    import spark.implicits._

    val byPhone = Seq("mobileNumber").map(col)
    val emailNull = $"emailAddress".isNull

    lazy val integratedWithExact = integratedContactPersons
      .filter(emailNull)
      .concatenateColumns("group", byPhone)
      .withColumn("inDelta", lit(false))

    lazy val newWithExact =
      dailyDeltaContactPersons
        .filter(emailNull)
        .concatenateColumns("group", byPhone)
        .withColumn("inDelta", lit(true))

    integratedWithExact
      .union(newWithExact)
      .addOhubId
      .drop("group")
      .selectLatestRecord
      .drop("inDelta")
      //      .removeSingletonGroups
      .as[ContactPerson]
  }
  private def determineExactMatchesEmailAndPhone(
    integratedContactPersons: Dataset[ContactPerson],
    dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): Dataset[ContactPerson] = {
    import spark.implicits._

    val mobileAndEmail = Seq("emailAddress", "mobileNumber").map(col)
    val emailOrPhoneNotNull = $"emailAddress".isNotNull || $"mobileNumber".isNotNull

    lazy val integratedWithExact = integratedContactPersons
      .filter(emailOrPhoneNotNull)
      .concatenateColumns("group", mobileAndEmail)
      .withColumn("inDelta", lit(false))

    lazy val newWithExact =
      dailyDeltaContactPersons
        .filter(emailOrPhoneNotNull)
        .concatenateColumns("group", mobileAndEmail)
        .withColumn("inDelta", lit(true))

    integratedWithExact
      .union(newWithExact)
      .addOhubId
      .drop("group")
      .selectLatestRecord
      .drop("inDelta")
      //      .removeSingletonGroups
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
