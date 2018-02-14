package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, EmOrderRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object EmOrderConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToEmOrderRecord(row: Row): EmOrderRecord = {
    EmOrderRecord(
      orderId = row.parseLongRangeOption(0),
      webServiceRequestId = row.parseLongRangeOption(1),
      countryCode = row.parseStringOption(2), // enum
      orderType = row.parseStringOption(3), // enum: Web
      transactionDate = row.parseDateTimeStampOption(4),
      orderAmount = row.parseLongRangeOption(5), // 0
      currencyCode = row.parseStringOption(6), // empty
      wholesaler = row.parseStringOption(7),
      orderUid = row.parseStringOption(8),
      campaignCode = row.parseStringOption(9),
      campaignName = row.parseStringOption(10),
      street = row.parseStringOption(11),
      houseNumber = row.parseStringOption(12),
      houseNumberAdd = row.parseStringOption(13), // empty
      zipCode = row.parseStringOption(14),
      city = row.parseStringOption(15),
      state = row.parseStringOption(16), // empty
      country = row.parseStringOption(17), // empty
      refOrderId = row.parseStringOption(18), // empty
      orderEmailAddress = row.parseStringOption(19), // empty
      orderPhoneNumber = row.parseStringOption(20), // empty
      orderMobilePhoneNumber = row.parseStringOption(21), // empty
      wholesalerLocation = row.parseStringOption(22), // empty
      wholesalerId = row.parseStringOption(23), // empty
      wholesalerCustomerNumber = row.parseStringOption(24), // empty
      invoiceName = row.parseStringOption(25), // empty
      invoiceStreet = row.parseStringOption(26), // empty
      invoiceHouseNumber = row.parseStringOption(27), // empty
      invoiceHouseNumberAdd = row.parseStringOption(28), // empty
      invoiceZipCode = row.parseStringOption(29), // empty
      invoiceCity = row.parseStringOption(30), // empty
      invoiceState = row.parseStringOption(31), // empty
      invoiceCountry = row.parseStringOption(32), // empty
      vat = row.parseStringOption(33), // empty
      comments = row.parseStringOption(34) // empty
    )
  }

  def transform(
    spark: SparkSession,
    records: Dataset[EmOrderRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[EmOrderRecord] = {
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
      .filter(_.length == 24)
      .map(rowToEmOrderRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, records, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
