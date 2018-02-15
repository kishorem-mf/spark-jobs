package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmLoyaltyBalanceRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmLoyaltyBalanceConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmLoyaltyBalanceRecord(row: Row): EmLoyaltyBalanceRecord = {
    EmLoyaltyBalanceRecord(
      countryCode = row.parseStringOption(0),
      totalLoyaltyPointsEarned = row.parseLongRangeOption(1),
      totalLoyaltyPointsSpent = row.parseLongRangeOption(2),
      totalLoyaltyPointsActual = row.parseLongRangeOption(3),
      totalLoyaltyPointsGoal = row.parseLongRangeOption(4))
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmLoyaltyBalanceRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmLoyaltyBalanceRecord] = {
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
      .map(rowToEmLoyaltyBalanceRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
