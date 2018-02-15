package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmActivityRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmActivityConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmActivityRecord(row: Row): EmActivityRecord = {
    EmActivityRecord(
      activityId = row.parseLongRangeOption(0),
      countryCode = row.parseStringOption(1),
      webServiceRequestId = row.parseStringOption(2),
      actionType = row.parseLongRangeOption(3),
      activityName = row.parseStringOption(4),
      activityDetails = row.parseStringOption(5)
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmActivityRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmActivityRecord] = {
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
      .map(rowToEmActivityRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
