package com.unilever.ohub.spark.tsv2parquet

import java.sql.Timestamp

import com.unilever.ohub.spark.storage.{ CountryRecord, Storage }
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

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
  private val csvColumnSeparator = "‰"

  private def rowToProductRecord(row: Row): ProductRecord = {
    val refProductId = row.parseStringOption(0)
    val source = row.parseStringOption(1)
    val countryCode = row.parseStringOption(2)
    val productConcatId = s"${countryCode.getOrElse("")}~${source.getOrElse("")}~${refProductId.getOrElse("")}"

    ProductRecord(
      PRODUCT_CONCAT_ID = productConcatId,
      REF_PRODUCT_ID = refProductId,
      SOURCE = source,
      COUNTRY_CODE = countryCode,
      STATUS = row.parseBooleanOption(3),
      STATUS_ORIGINAL = row.parseStringOption(3),
      DATE_CREATED = row.parseDateTimeStampOption(4),
      DATE_CREATED_ORIGINAL = row.parseStringOption(4),
      DATE_MODIFIED = row.parseDateTimeStampOption(5),
      DATE_MODIFIED_ORIGINAL = row.parseStringOption(5),
      PRODUCT_NAME = row.parseStringOption(6),
      EAN_CU = row.parseStringOption(7),
      EAN_DU = row.parseStringOption(8),
      MRDR = row.parseStringOption(9),
      UNIT = row.parseStringOption(10),
      UNIT_PRICE = row.parseBigDecimalOption(11),
      UNIT_PRICE_ORIGINAL = row.parseStringOption(11),
      UNIT_PRICE_CURRENCY = row.parseStringOption(12)
    )
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
        productRecords("COUNTRY_CODE") === countryRecords("COUNTRY_CODE"),
        JoinType.LeftOuter
      )
      .map {
        case (productRecord, countryRecord) => Option(countryRecord).fold(productRecord) { cr =>
          productRecord.copy(
            COUNTRY_CODE = Option(cr.COUNTRY_CODE)
          )
        }
      }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating orders parquet from [$inputFile] to [$outputFile]")

    val productRecords = storage
      .readFromCSV(inputFile, separator = csvColumnSeparator)
      .filter(_.length == 13)
      .map(rowToProductRecord)

    val countryRecords = storage.countries

    val transformed = transform(spark, productRecords, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = "COUNTRY_CODE")
  }
}
