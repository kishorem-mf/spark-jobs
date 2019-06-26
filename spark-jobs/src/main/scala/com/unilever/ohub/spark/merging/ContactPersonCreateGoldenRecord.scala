package com.unilever.ohub.spark.merging

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DefaultConfig
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.expressions.{ Window, WindowSpec }
import org.apache.spark.sql.functions._

object ContactPersonCreateGoldenRecord extends BaseMerging[ContactPerson] {

  private val consentFlagDateMapping = Map(
    "hasEmailOptIn" -> "emailOptInDate",
    "hasEmailDoubleOptIn" -> "emailDoubleOptInDate",
    "hasMobileOptIn" -> "mobileOptInDate",
    "hasMobileDoubleOptIn" -> "mobileDoubleOptInDate"
  )

  private[merging] def pickLatestConsent(
    spark: SparkSession,
    df: DataFrame,
    column: String
  ): DataFrame = {
    import spark.implicits._

    val groupWindow = Window.partitionBy($"ohubId")
    val consentOrderDateColumn = consentFlagDateMapping(column)
    val consentOrderDateColumnTemp = consentOrderDateColumn + "TEMP"

    // Only for ordering: use a consentOrderDateColumnTemp where the related OptIn null values are set to 1970-01-01
    val consentOrderWindow = groupWindow.orderBy(
      col(consentOrderDateColumnTemp).desc_nulls_last,
      $"dateUpdated".desc_nulls_last,
      $"dateCreated".desc_nulls_last,
      $"ohubUpdated".desc
    )

    df.withColumn(
      consentOrderDateColumnTemp, when(
      col(column).isNull, Timestamp.valueOf("1970-01-01 00:00:00")
    ).otherwise(col(consentOrderDateColumn))
    )
      .withColumn(prefixNewColumn + column, first(col(column), true).over(
        consentOrderWindow.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
      .withColumn(prefixNewColumn + consentOrderDateColumn, first(col(consentOrderDateColumn), true).over(
        consentOrderWindow.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
  }

  override def setFieldsToLatestValue(spark: SparkSession, orderByDatesWindow: WindowSpec,
    dataframe: DataFrame, excludeFields: Seq[String] = Seq(),
    reversedOrderColumns: Seq[String] = Seq()): DataFrame = {
    // Set all columns of dataset on the first value of it's newestNotNullWindow
    // Note: we write the result to a new column as prefix+columnName because overwriting introduces randomness
    // OptiIn Logic: for each has{}OptIn we pick the latest value and it's related date

    val columns = dataframe.columns.filter(!excludeFields.contains(_))
    val columnsModified = columns.map(column ⇒ prefixNewColumn + column)
    val skipColumns = consentFlagDateMapping.valuesIterator.toList
    val consentFlagColumns = consentFlagDateMapping.keySet

    var dataframeModified: DataFrame = columns
      .foldLeft(dataframe)(
        (cp: DataFrame, column: String) ⇒ {
          if (reversedOrderColumns.contains(column)) pickOldest(spark, cp, column)
          else if (skipColumns.contains(column)) cp //Skip the consentDate cols: are elaborated next with Consent Flags
          else if (consentFlagColumns.contains(column)) pickLatestConsent(spark, cp, column)
          else pickNewest(spark, cp, column, orderByDatesWindow)
        }
      )

    dataframeModified
      .select(columnsModified.map(c ⇒ col(c)): _*)
      .toDF(columns: _*)
  }

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    log.info(s"Creating golden contact persons records based on [${config.inputFile}] and writing them to [${config.outputFile}]")

    val entity = storage.readFromParquet[ContactPerson](config.inputFile)

    val transformed = transform(spark, entity)

    storage.writeToParquet(transformed, config.outputFile)
  }

}
