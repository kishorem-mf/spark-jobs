package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.merging.DataFrameHelpers._
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import scopt.OptionParser
import com.unilever.ohub.spark.merging.ContactPersonExactMatchUtils._

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

    val matchedExactEmailAndPhone: Dataset[ContactPerson] = determineExactMatchOnEmailAndMobile(
      integratedContactPersons,
      dailyDeltaContactPersons)

    val unmatchedEmailAndPhoneIntegrated = integratedContactPersons
      .join(matchedExactEmailAndPhone, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    val unmatchedEmailAndPhoneDelta = dailyDeltaContactPersons
      .join(matchedExactEmailAndPhone, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    val groupingColumns = Seq("countryCode", "city", "street", "houseNumber", "houseNumberExtension", "zipCode", "firstName", "lastName")
    val notNullColumns = Seq("firstName", "lastName", "street")
    val addressColumns = Seq("houseNumber", "houseNumberExtension", "zipCode").map(col)

    val matchedExactLocation: Dataset[ContactPerson] = matchColumns[ContactPerson](
      unmatchedEmailAndPhoneIntegrated,
      unmatchedEmailAndPhoneDelta,
      groupingColumns,
      notNullColumns)
      .concatenateColumns("location", addressColumns)
      .filter($"location".notEqual(""))
      .drop("location")
      .as[ContactPerson]

    val matchedExactAll = matchedExactEmailAndPhone
      .unionByName(matchedExactLocation)

    val oneRecordPerMatchingGroup = matchedExactAll
      .grabOneRecordPerGroup
      .as[ContactPerson]

    val unmatchedIntegrated = integratedContactPersons
      .join(matchedExactAll, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]
      .unionByName(oneRecordPerMatchingGroup)

    val unmatchedDelta = dailyDeltaContactPersons
      .join(matchedExactAll, Seq("concatId"), JoinType.LeftAnti)
      .as[ContactPerson]

    (matchedExactAll, unmatchedIntegrated, unmatchedDelta)
  }

 /* private def determineExactMatches(
    exactMatchColumn: String,
    integratedContactPersons: Dataset[ContactPerson],
    dailyDeltaContactPersons: Dataset[ContactPerson])(implicit spark: SparkSession): Dataset[ContactPerson] = {
    import spark.implicits._

    val exactMatchColumnSeq = Seq(exactMatchColumn).map(col)

    lazy val integratedWithExact = integratedContactPersons
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .withColumn("inDelta", lit(false))

    lazy val newWithExact = dailyDeltaContactPersons
      .columnsNotNullAndNotEmpty(exactMatchColumnSeq)
      .withColumn("inDelta",lit(true))

    integratedWithExact
      .union(newWithExact)
      .addOhubIdForCP(exactMatchColumn)
      .selectLatestRecord
      .drop("inDelta")
      .as[ContactPerson]
  }*/

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
