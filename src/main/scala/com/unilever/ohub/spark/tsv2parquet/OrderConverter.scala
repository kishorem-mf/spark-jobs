package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.storage.{ CountryRecord, Storage }
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.{ Dataset, SparkSession }

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
  def checkOrderType(orderType: String): Option[String] = {
    if (Seq(
      "", "SSD", "TRANSFER", "DIRECT", "UNKNOWN", "MERCHANDISE", "SAMPLE", "EVENT", "WEB", "BIN", "OTHER"
    ).contains(orderType)) {
      Some(orderType)
    } else {
      throw new IllegalArgumentException("Unknown ORDER_TYPE: " + orderType)
    }
  }

  def linePartsToOrderRecord(lineParts: Array[String]): OrderRecord = {
    try {
      OrderRecord(
        ORDER_CONCAT_ID = s"${lineParts(2)}~${lineParts(1)}~${lineParts(0)}",
        REF_ORDER_ID = parseStringOption(lineParts(0)),
        SOURCE = parseStringOption(lineParts(1)),
        COUNTRY_CODE = parseStringOption(lineParts(2)),
        STATUS = parseBoolOption(lineParts(3)),
        STATUS_ORIGINAL = parseStringOption(lineParts(3)),
        REF_OPERATOR_ID = parseStringOption(lineParts(4)),
        REF_CONTACT_PERSON_ID = parseStringOption(lineParts(5)),
        ORDER_TYPE = checkOrderType(lineParts(6)),
        TRANSACTION_DATE = parseDateTimeStampOption(lineParts(7)),
        TRANSACTION_DATE_ORIGINAL = parseStringOption(lineParts(7)),
        REF_PRODUCT_ID = parseStringOption(lineParts(8)),
        QUANTITY = parseLongRangeOption(lineParts(9)),
        QUANTITY_ORIGINAL = parseStringOption(lineParts(9)),
        ORDER_LINE_VALUE = parseBigDecimalOption(lineParts(10)),
        ORDER_LINE_VALUE_ORIGINAL = parseStringOption(lineParts(10)),
        ORDER_VALUE = parseBigDecimalOption(lineParts(11)),
        ORDER_VALUE_ORIGINAL = parseStringOption(lineParts(11)),
        WHOLESALER = parseStringOption(lineParts(12)),
        CAMPAIGN_CODE = parseStringOption(lineParts(13)),
        CAMPAIGN_NAME = parseStringOption(lineParts(14)),
        UNIT_PRICE = parseBigDecimalOption(lineParts(15)),
        UNIT_PRICE_ORIGINAL = parseStringOption(lineParts(15)),
        CURRENCY_CODE = parseStringOption(lineParts(16)),
        DATE_CREATED = parseDateTimeStampOption(lineParts(17)),
        DATE_MODIFIED = parseDateTimeStampOption(lineParts(18))
      )
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
    }
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
        countryRecords("CURRENCY_CODE").isNotNull and
          orderRecords("COUNTRY_CODE") === countryRecords("COUNTRY_CODE"),
        LeftOuter.sql
      )
      .map {
        case (orderRecord, countryRecord) => orderRecord.copy(
          COUNTRY_CODE = Some(countryRecord.COUNTRY_CODE),
          CURRENCY_CODE = Some(countryRecord.CURRENCY_CODE)
        )
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    val hasValidLineLength = CustomParsers.hasValidLineLength(19) _

    val orderRecords = storage
      .readFromCSV[String](inputFile)
      .map(_.split("‰", -1))
      .filter(hasValidLineLength)
      .map(linePartsToOrderRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, orderRecords, countryRecords)

    storage.writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
