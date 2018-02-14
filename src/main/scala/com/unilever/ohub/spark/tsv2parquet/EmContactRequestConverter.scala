package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmContactRequestRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmContactRequestConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmContactRequestRecord(row: Row): EmContactRequestRecord = {
    EmContactRequestRecord(
      contactRequestId = row.parseLongRangeOption(0),
      countryCode = row.parseStringOption(1),
      webServiceRequestId = row.parseLongRangeOption(2),
      contactMessage = row.parseStringOption(3),
      happy = row.parseStringOption(4),
      requestType = row.parseLongRangeOption(5) // enum
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmContactRequestRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmContactRequestRecord] = {
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
      .filter(_.length == 6)
      .map(rowToEmContactRequestRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
