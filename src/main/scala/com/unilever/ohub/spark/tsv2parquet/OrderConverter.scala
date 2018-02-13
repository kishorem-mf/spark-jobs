package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.storage.{ CountryRecord, Storage }
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.LeftOuter
import CustomParsers.Implicits._
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

case class OrderRecord(
  ORDER_CONCAT_ID: String, REF_ORDER_ID: Option[String], SOURCE: Option[String],
  COUNTRY_CODE: Option[String], STATUS: Option[Boolean], STATUS_ORIGINAL: Option[String],
  REF_OPERATOR_ID: Option[String],
  REF_CONTACT_PERSON_ID: Option[String], ORDER_TYPE: Option[String], TRANSACTION_DATE: Option[Timestamp],
  TRANSACTION_DATE_ORIGINAL: Option[String], REF_PRODUCT_ID: Option[String], QUANTITY: Option[Long],
  QUANTITY_ORIGINAL: Option[String],
  ORDER_LINE_VALUE: Option[BigDecimal], ORDER_LINE_VALUE_ORIGINAL: Option[String],
  ORDER_VALUE: Option[BigDecimal], ORDER_VALUE_ORIGINAL: Option[String], WHOLESALER: Option[String],
  CAMPAIGN_CODE: Option[String], CAMPAIGN_NAME: Option[String],
  UNIT_PRICE: Option[BigDecimal], UNIT_PRICE_ORIGINAL: Option[String], CURRENCY_CODE: Option[String],
  DATE_CREATED: Option[Timestamp], DATE_MODIFIED: Option[Timestamp]
)

object OrderConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  def checkOrderType(orderType: String): Option[String] = {
    if (Seq(
      "", "SSD", "TRANSFER", "DIRECT", "UNKNOWN", "MERCHANDISE", "SAMPLE", "EVENT", "WEB", "BIN", "OTHER"
    ).contains(orderType)) {
      Some(orderType)
    } else {
      throw new IllegalArgumentException("Unknown ORDER_TYPE: " + orderType)
    }
  }

  def rowToOrderRecord(row: Row): OrderRecord = {
    val refOrderId = row.parseStringOption(0)
    val source = row.parseStringOption(1)
    val countryCode = row.parseStringOption(2)
    val orderConcatId = s"${countryCode.getOrElse("")}~${source.getOrElse("")}~${refOrderId.getOrElse("")}"

    OrderRecord(
      ORDER_CONCAT_ID = orderConcatId,
      REF_ORDER_ID = refOrderId,
      SOURCE = source,
      COUNTRY_CODE = countryCode,
      STATUS = row.parseBooleanOption(3),
      STATUS_ORIGINAL = row.parseStringOption(3),
      REF_OPERATOR_ID = row.parseStringOption(4),
      REF_CONTACT_PERSON_ID = row.parseStringOption(5),
      ORDER_TYPE = row.parseStringOption(6).flatMap(checkOrderType),
      TRANSACTION_DATE = row.parseDateTimeStampOption(7),
      TRANSACTION_DATE_ORIGINAL = row.parseStringOption(7),
      REF_PRODUCT_ID = row.parseStringOption(8),
      QUANTITY = row.parseLongRangeOption(9),
      QUANTITY_ORIGINAL = row.parseStringOption(9),
      ORDER_LINE_VALUE = row.parseBigDecimalOption(10),
      ORDER_LINE_VALUE_ORIGINAL = row.parseStringOption(10),
      ORDER_VALUE = row.parseBigDecimalOption(11),
      ORDER_VALUE_ORIGINAL = row.parseStringOption(11),
      WHOLESALER = row.parseStringOption(12),
      CAMPAIGN_CODE = row.parseStringOption(13),
      CAMPAIGN_NAME = row.parseStringOption(14),
      UNIT_PRICE = row.parseBigDecimalOption(15),
      UNIT_PRICE_ORIGINAL = row.parseStringOption(15),
      CURRENCY_CODE = row.parseStringOption(16),
      DATE_CREATED = row.parseDateTimeStampOption(17),
      DATE_MODIFIED = row.parseDateTimeStampOption(18)
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
        orderRecords("COUNTRY_CODE") === countryRecords("COUNTRY_CODE"),
        LeftOuter
      )
      .map {
        case (orderRecord, countryRecord) => Option(countryRecord).fold(orderRecord) { cr =>
          orderRecord.copy(
            COUNTRY_CODE = Option(cr.COUNTRY_CODE),
            CURRENCY_CODE = Option(cr.CURRENCY_CODE)
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

    storage.writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
