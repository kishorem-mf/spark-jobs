package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmResDownloadRequestRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmResDownloadRequestConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmResDownloadRequestRecord(row: Row): EmResDownloadRequestRecord = {
    EmResDownloadRequestRecord(
      resDownloadRequestId = row.parseLongRangeOption(0),
      countryCode = row.parseStringOption(1), // enum
      webServiceRequestId = row.parseLongRangeOption(2),
      downloadItem = row.parseStringOption(3)
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmResDownloadRequestRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmResDownloadRequestRecord] = {
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
      .filter(_.length == 4)
      .map(rowToEmResDownloadRequestRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
