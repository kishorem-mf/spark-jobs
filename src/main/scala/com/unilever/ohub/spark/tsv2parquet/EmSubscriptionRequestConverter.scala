package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmSubscriptionRequestRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmSubscriptionRequestConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmSubscriptionRequestRecord(row: Row): EmSubscriptionRequestRecord = {
    EmSubscriptionRequestRecord(
      subscriptionRequestId = row.parseLongRangeOption(0),
      countryCode = row.parseStringOption(1), // enum
      webServiceRequestId = row.parseLongRangeOption(2),
      newsletter = row.parseStringOption(3), // enum: default_newsletter_opt_in
      subscribed = row.parseBooleanOption(4), // 0/1
      subscriptionDate = row.parseDateTimeStampOption(5),
      subscriptionConfirmed = row.parseBooleanOption(6), // 0/1
      subscriptionConfirmedDate = row.parseDateTimeStampOption(7)
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmSubscriptionRequestRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmSubscriptionRequestRecord] = {
    import spark.implicits._

    records
      .joinWith(
        countryRecords,
        records("countryCode") === countryRecords("countryCode"),
        JoinType.LeftOuter
      )
      .map {
        case (record, countryRecord) => Option(countryRecord).fold(record) { cr =>
          record.copy(
            countryCode = Option(cr.countryCode)
          )
        }
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating orders parquet from [$inputFile] to [$outputFile]")

    val records = storage
      .readFromCSV(inputFile, separator = csvColumnSeparator)
      .filter(_.length == 8)
      .map(rowToEmSubscriptionRequestRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
