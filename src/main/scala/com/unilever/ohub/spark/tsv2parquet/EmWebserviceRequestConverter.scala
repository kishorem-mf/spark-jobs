package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmWebserviceRequestRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmWebserviceRequestConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmWebserviceRequestRecord(row: Row): EmWebserviceRequestRecord = {
    EmWebserviceRequestRecord(
      webServiceRequestId = row.parseLongRangeOption(0),
      countryCode = row.parseStringOption(1), // enum
      username = row.parseStringOption(2), // enum: emakina
      requestIdentifier = row.parseStringOption(3),
      emSourceId = row.parseStringOption(4),
      dateReceived = row.parseDateTimeStampOption(5),
      updateContext = row.parseStringOption(6), // enum: update_profile, register_profile
      operation = row.parseStringOption(7), // enum
      ipAddress = row.parseStringOption(8),
      processedFlag = row.parseBooleanOption(9), // 0/1
      dateWodProcessed = row.parseDateTimeStampOption(10),
      indSkipped = row.parseBooleanOption(11), // Y/N
      endRequestDate = row.parseDateTimeStampOption(12),
      compositeVersion = row.parseStringOption(13), // 1.2~1.4
      skipReason = row.parseStringOption(14), // enum: NOT_LATEST
      processId = row.parseLongRangeOption(15)
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmWebserviceRequestRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmWebserviceRequestRecord] = {
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
      .filter(_.length == 16)
      .map(rowToEmWebserviceRequestRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
