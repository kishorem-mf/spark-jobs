package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.DefaultConfig
import com.unilever.ohub.spark.domain.entity.ContactPersonGolden
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Consent(
    optInFields: List[String] = List(),
    optOutFields: List[String] = List(),
    sortingDateFields: List[String] = List(),
    furtherSortingDateFields: List[String] = List("dateUpdated", "dateCreated", "ohubUpdated")
)

object ContactPersonCreatePerfectGoldenRecord extends BaseMerging[ContactPersonGolden] {

  val colOhubId = "ohubid"
  val colCounterField = "group_row_num"

  val consentFieldsByChannel: Map[String, Consent] = Map(

    "email" -> Consent(
      optInFields = List("hasEmailDoubleOptIn", "hasEmailOptIn"),
      optOutFields = List("hasEmailOptOut"),
      sortingDateFields = List("emailOptOutDate","emailDoubleOptInDate", "emailOptInDate")
    ),

    "mobile" -> Consent(
      optInFields = List("hasMobileDoubleOptIn", "hasMobileOptIn"),
      optOutFields = List("hasMobileOptOut"),
      sortingDateFields = List("mobileDoubleOptInDate", "mobileOptInDate")
    ),

    "directMail" -> Consent(
      optInFields = List("hasDirectMailOptIn"),
      optOutFields = List("hasDirectMailOptOut")
    ),

    "fax" -> Consent(
      optInFields = List("hasFaxOptIn"),
      optOutFields = List("hasFaxOptOut")
    ),

    "telemarketing" -> Consent(
      optInFields = List("hasTelemarketingOptIn"),
      optOutFields = List("hasTelemarketingOptOut")
    )

  )

  def mergeConsent(spark: SparkSession, dfChannel: DataFrame, channel: String): DataFrame = {

    import spark.implicits._

    val baseDateSetsForSorting = consentFieldsByChannel(channel).furtherSortingDateFields
    val retainColumns =
      List(colOhubId) ++
        consentFieldsByChannel(channel).optInFields ++
        consentFieldsByChannel(channel).optOutFields ++
        consentFieldsByChannel(channel).sortingDateFields ++
        consentFieldsByChannel(channel).furtherSortingDateFields

    val groupWindow = Window.partitionBy(col(colOhubId))

    val sortingFields =
      consentFieldsByChannel(channel).sortingDateFields ++
        consentFieldsByChannel(channel).furtherSortingDateFields

    val orderByDatesWindow = groupWindow.orderBy(
      sortingFields.map(c ⇒ col(c).desc_nulls_last): _*
    )

    dfChannel
      .filter($"isActive")
      .select(retainColumns.map(c ⇒ col(c)): _*) // Take only the columns necessary for the merging of the specific channel
      .withColumn(colCounterField, row_number().over(orderByDatesWindow))
      .filter(col(colCounterField) === 1)
      .drop(colCounterField)
      .drop(baseDateSetsForSorting: _*)
  }

  override def transform(spark: SparkSession, contactPersons: Dataset[ContactPersonGolden]): Dataset[ContactPersonGolden] = {

    import spark.implicits._

    val channels = List("email", "mobile", "telemarketing", "fax", "directMail")

    val dfMergedMostRecent: Dataset[ContactPersonGolden] = super.transform(spark, contactPersons)

    // Remove colums that will be filled in later
    var removeColumns :List[String] = List.empty
    for (channel ← channels) {
      removeColumns ++=
        consentFieldsByChannel(channel).optInFields ++
          consentFieldsByChannel(channel).optOutFields ++
          consentFieldsByChannel(channel).sortingDateFields
    }

    var tempDF = dfMergedMostRecent.drop(removeColumns: _*)

    // Fill in the channel fields for each channel
    for (channel ← channels) {
      val dfMergedByChannel: DataFrame = mergeConsent(spark, contactPersons.toDF(), channel)
      tempDF = tempDF.join(dfMergedByChannel, Seq(colOhubId), "inner")
    }

    tempDF.as[ContactPersonGolden]
  }

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    log.info(s"Creating golden contact persons records based on [${config.inputFile}] and writing them to [${config.outputFile}]")

    val entity = storage.readFromParquet[ContactPersonGolden](config.inputFile)

    val transformed = transform(spark, entity)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
