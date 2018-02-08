package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.storage.{ CountryRecord, Storage }
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.LeftOuter
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.{ Dataset, SparkSession }

case class ProductRecord(
  PRODUCT_CONCAT_ID: String, REF_PRODUCT_ID: Option[String], SOURCE: Option[String],
  COUNTRY_CODE: Option[String], STATUS: Option[Boolean], STATUS_ORIGINAL: Option[String],
  DATE_CREATED: Option[Timestamp],
  DATE_CREATED_ORIGINAL: Option[String], DATE_MODIFIED: Option[Timestamp],
  DATE_MODIFIED_ORIGINAL: Option[String], PRODUCT_NAME: Option[String], EAN_CU: Option[String],
  EAN_DU: Option[String],
  MRDR: Option[String], UNIT: Option[String], UNIT_PRICE: Option[BigDecimal],
  UNIT_PRICE_ORIGINAL: Option[String], UNIT_PRICE_CURRENCY: Option[String]
)

object ProductConverter extends SparkJob {
  private def linePartsToProductRecord(lineParts: Array[String]): ProductRecord = {
    try {
      ProductRecord(
        PRODUCT_CONCAT_ID = s"${lineParts(2)}~${lineParts(1)}~${lineParts(0)}",
        REF_PRODUCT_ID = parseStringOption(lineParts(0)),
        SOURCE = parseStringOption(lineParts(1)),
        COUNTRY_CODE = parseStringOption(lineParts(2)),
        STATUS = parseBoolOption(lineParts(3)),
        STATUS_ORIGINAL = parseStringOption(lineParts(3)),
        DATE_CREATED = parseDateTimeStampOption(lineParts(4)),
        DATE_CREATED_ORIGINAL = parseStringOption(lineParts(4)),
        DATE_MODIFIED = parseDateTimeStampOption(lineParts(5)),
        DATE_MODIFIED_ORIGINAL = parseStringOption(lineParts(5)),
        PRODUCT_NAME = parseStringOption(lineParts(6)),
        EAN_CU = parseStringOption(lineParts(7)),
        EAN_DU = parseStringOption(lineParts(8)),
        MRDR = parseStringOption(lineParts(9)),
        UNIT = parseStringOption(lineParts(10)),
        UNIT_PRICE = parseBigDecimalOption(lineParts(11)),
        UNIT_PRICE_ORIGINAL = parseStringOption(lineParts(11)),
        UNIT_PRICE_CURRENCY = parseStringOption(lineParts(12))
      )
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Exception while parsing line: ${lineParts.mkString("‰")}", e)
    }
  }

  def transform(
    spark: SparkSession,
    productRecords: Dataset[ProductRecord],
    countryRecords: Dataset[CountryRecord]
  ): Dataset[ProductRecord] = {
    import spark.implicits._

    productRecords
      .joinWith(
        countryRecords,
        countryRecords("CURRENCY_CODE").isNotNull and
          productRecords("COUNTRY_CODE") === countryRecords("COUNTRY_CODE"),
        LeftOuter
      )
      .map {
        case (productRecord, countryRecord) => productRecord.copy(
          COUNTRY_CODE = Some(countryRecord.COUNTRY_CODE)
        )
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    val expectedPartCount = 13
    val hasValidLineLength = CustomParsers.hasValidLineLength(expectedPartCount) _

    val productRecords = storage
      .readFromCSV[String](inputFile)
      .map(_.split("‰", -1))
      .filter(hasValidLineLength)
      .map(linePartsToProductRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, productRecords, countryRecords)

    storage.writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
