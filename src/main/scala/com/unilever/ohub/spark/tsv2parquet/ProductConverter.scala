package com.unilever.ohub.spark.tsv2parquet

import java.io.InputStream
import java.sql.Timestamp

import com.unilever.ohub.spark.generic.FileSystems
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.log4j.{ LogManager, Logger }
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{ DataFrame, SparkSession }

import scala.io.Source

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

object ProductConverter extends App {
  implicit private val log: Logger = LogManager.getLogger(getClass)

  val (inputFile, outputFile) = FileSystems.getFileNames(args, "INPUT_FILE", "OUTPUT_FILE")

  log.info(s"Generating orders parquet from [$inputFile] to [$outputFile]")

  val spark = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

  import spark.implicits._

  val inputStream: InputStream = getClass.getResourceAsStream("/country_codes.csv")
  val readSeq: Seq[String] = Source.fromInputStream(inputStream).getLines().toSeq
  val countryRecordsDF: DataFrame = spark.sparkContext.parallelize(readSeq)
    .toDS()
    .map(_.split(","))
    .map(cells => (parseStringOption(cells(6)), parseStringOption(cells(2)), parseStringOption(cells(9))))
    .filter(_ != ("ISO3166_1_Alpha_2", "official_name_en", "ISO4217_currency_alphabetic_code"))
    .toDF("COUNTRY_CODE", "COUNTRY", "CURRENCY_CODE")

  val expectedPartCount = 13
  val hasValidLineLength = CustomParsers.hasValidLineLength(expectedPartCount) _

  val startOfJob = System.currentTimeMillis()
  val lines = spark.read.textFile(inputFile)

  def linePartsToProductRecord(lineParts: Array[String]): ProductRecord = {
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

  val recordsDF: DataFrame = lines
    .filter(_.nonEmpty)
    .filter(!_.startsWith("REF_PRODUCT_ID"))
    .map(_.split("‰", -1))
    .filter(hasValidLineLength)
    .map(linePartsToProductRecord)
    .toDF()

  recordsDF.createOrReplaceTempView("PRODUCTS")
  countryRecordsDF.createOrReplaceTempView("COUNTRIES")
  val joinedRecordsDF: DataFrame = spark.sql(
    """
      |SELECT PDT.PRODUCT_CONCAT_ID,PDT.REF_PRODUCT_ID,PDT.SOURCE,PDT.COUNTRY_CODE,PDT.STATUS,PDT.STATUS_ORIGINAL,PDT.DATE_CREATED,PDT.DATE_CREATED_ORIGINAL,PDT.DATE_MODIFIED,PDT.DATE_MODIFIED_ORIGINAL,PDT.PRODUCT_NAME,PDT.EAN_CU,PDT.EAN_DU,PDT.MRDR,PDT.UNIT,PDT.UNIT_PRICE,PDT.UNIT_PRICE_ORIGINAL,CTR.CURRENCY_CODE UNIT_PRICE_CURRENCY
      |FROM PRODUCTS PDT
      |LEFT JOIN COUNTRIES CTR
      | ON PDT.COUNTRY_CODE = CTR.COUNTRY_CODE
      |WHERE CTR.CURRENCY_CODE IS NOT NULL
    """.stripMargin)

  joinedRecordsDF.write.mode(Overwrite).partitionBy("COUNTRY_CODE").format("parquet").save(outputFile)

  log.debug(joinedRecordsDF.schema.treeString)
  log.info(s"Done in ${(System.currentTimeMillis - startOfJob) / 1000}s")
}
