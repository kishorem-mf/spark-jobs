package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmSampleOrderRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmSampleOrderConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmSampleOrderRecord(row: Row): EmSampleOrderRecord = {
    EmSampleOrderRecord(
      sampleOrderId = row.parseLongRangeOption(0),
      countryCode = row.parseStringOption(1), // enum
      webServiceRequestId = row.parseLongRangeOption(2),
      itemType = row.parseStringOption(3), // enum: Sample
      termsAgreed = row.parseBooleanOption(4), // 1
      itemId = row.parseLongRangeOption(5),
      orderItem = row.parseStringOption(6), // enum: Product
      quantity = row.parseLongRangeOption(7), // 1
      street = row.parseStringOption(8),
      houseNumber = row.parseStringOption(9),
      houseNumberExt = row.parseStringOption(10), // empty
      postcode = row.parseStringOption(11),
      city = row.parseStringOption(12),
      state = row.parseStringOption(13), // empty
      country = row.parseStringOption(14)
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmSampleOrderRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmSampleOrderRecord] = {
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
      .filter(_.length == 15)
      .map(rowToEmSampleOrderRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
