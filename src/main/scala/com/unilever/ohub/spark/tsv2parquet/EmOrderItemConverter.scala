package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmOrderItemRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmOrderItemConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmOrderItemRecord(row: Row): EmOrderItemRecord = {
    EmOrderItemRecord(
      orderLineId = row.parseLongRangeOption(0),
      countryCode = row.parseStringOption(1), // enum
      refOrderLineId = row.parseBooleanOption(2), // 0/1
      orderFk = row.parseLongRangeOption(3),
      quantity = row.parseBooleanOption(4), // 1
      amount = row.parseBooleanOption(5), // 0
      unitPrice = row.parseBooleanOption(6), // 0
      unitPriceCurrency = row.parseStringOption(7), // empty
      itemId = row.parseLongRangeOption(8),
      itemType = row.parseStringOption(9), // Sample
      itemName = row.parseStringOption(10),
      eanCustomerUnit = row.parseStringOption(11), // ?, empty
      eanDispatchUnit = row.parseStringOption(12), // ?, empty
      mrdrCode = row.parseStringOption(13), // ?, empty
      category = row.parseStringOption(14), // ?, empty
      subCategory = row.parseStringOption(15), // ?, empty
      brand = row.parseStringOption(16), // ?, empty
      subBrand = row.parseStringOption(17), // ?, empty
      itemUnit = row.parseStringOption(18), // ?, empty
      itemUnitPrice = row.parseLongRangeOption(19), // 0
      itemUnitPriceCurrency = row.parseStringOption(20), // ?, empty
      loyaltyPoints = row.parseStringOption(21), // ?, empty
      campaignLabel = row.parseStringOption(22), // ?, empty
      comments = row.parseStringOption(23) // ?, empty
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmOrderItemRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmOrderItemRecord] = {
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
      .filter(_.length == 35)
      .map(rowToEmOrderItemRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
