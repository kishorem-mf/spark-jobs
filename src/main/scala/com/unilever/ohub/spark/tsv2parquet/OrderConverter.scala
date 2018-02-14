package com.unilever.ohub.spark.tsv2parquet

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, OrderRecord }
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object OrderConverter extends SparkJob {
  private val csvColumnSeparator = "‰"
  private val knownOrderTypes =
    Seq("", "SSD", "TRANSFER", "DIRECT", "UNKNOWN", "MERCHANDISE", "SAMPLE", "EVENT", "WEB", "BIN", "OTHER")

  def checkOrderType(orderType: String): Option[String] = {
    if (knownOrderTypes.contains(orderType)) {
      Some(orderType)
    } else {
      throw new IllegalArgumentException("Unknown orderType: " + orderType)
    }
  }

  def rowToOrderRecord(row: Row): OrderRecord = {
    val refOrderId = row.parseStringOption(0)
    val source = row.parseStringOption(1)
    val countryCode = row.parseStringOption(2)
    val orderConcatId = s"${countryCode.getOrElse("")}~${source.getOrElse("")}~${refOrderId.getOrElse("")}"

    OrderRecord(
      orderConcatId = orderConcatId,
      refOrderId = refOrderId,
      source = source,
      countryCode = countryCode,
      status = row.parseBooleanOption(3),
      statusOriginal = row.parseStringOption(3),
      refOperatorId = row.parseStringOption(4),
      refContactPersonId = row.parseStringOption(5),
      orderType = row.parseStringOption(6).flatMap(checkOrderType),
      transactionDate = row.parseDateTimeStampOption(7),
      transactionDateOriginal = row.parseStringOption(7),
      refProductId = row.parseStringOption(8),
      quantity = row.parseLongRangeOption(9),
      quantityOriginal = row.parseStringOption(9),
      orderLineValue = row.parseBigDecimalOption(10),
      orderLineValueOriginal = row.parseStringOption(10),
      orderValue = row.parseBigDecimalOption(11),
      orderValueOriginal = row.parseStringOption(11),
      wholesaler = row.parseStringOption(12),
      campaignCode = row.parseStringOption(13),
      campaignName = row.parseStringOption(14),
      unitPrice = row.parseBigDecimalOption(15),
      unitPriceOriginal = row.parseStringOption(15),
      currencyCode = row.parseStringOption(16),
      dateCreated = row.parseDateTimeStampOption(17),
      dateModified = row.parseDateTimeStampOption(18)
    )
  }

  def transform(
    spark: SparkSession,
    orderRecords: Dataset[OrderRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[OrderRecord] = {
    import spark.implicits._

    orderRecords
      .joinWith(
        countryRecords,
        orderRecords("countryCode") === countryRecords("countryCode"),
        JoinType.LeftOuter
      )
      .map {
        case (orderRecord, countryRecord) => Option(countryRecord).fold(orderRecord) { cr =>
          orderRecord.copy(
            countryCode = Option(cr.countryCode),
            currencyCode = Option(cr.currencyCode)
          )
        }
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating orders parquet from [$inputFile] to [$outputFile]")

    val orderRecords = storage
      .readFromCSV(inputFile, separator = csvColumnSeparator)
      .filter(_.length == 19)
      .map(rowToOrderRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, orderRecords, countryRecords)

    storage.writeToParquet(transformed, outputFile, partitionBy = "countryCode")
  }
}
