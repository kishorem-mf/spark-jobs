package com.unilever.ohub.spark.tsv2parquet

import java.util.InputMismatchException

import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.sql.JoinType
import CustomParsers.Implicits._
import com.unilever.ohub.spark.data.{ CountryRecord, ProductRecord }
import com.unilever.ohub.spark.generic.StringFunctions
import org.apache.spark.sql.{ Dataset, Row, SparkSession }

object ProductConverter extends SparkJob {
  private val csvColumnSeparator = "‰"

  private def rowToProductRecord(row: Row): ProductRecord = {
    val refProductId = row.parseStringOption(0)
    val source = row.parseStringOption(1)
    val countryCode = row.parseStringOption(2)
    val productConcatId = StringFunctions.createConcatId(countryCode, source, refProductId)

    ProductRecord(
      productConcatId = productConcatId,
      refProductId = refProductId,
      source = source,
      countryCode = countryCode,
      status = row.parseBooleanOption(3),
      statusOriginal = row.parseStringOption(3),
      dateCreated = row.parseDateTimeStampOption(4),
      dateCreatedOriginal = row.parseStringOption(4),
      dateModified = row.parseDateTimeStampOption(5),
      dateModifiedOriginal = row.parseStringOption(5),
      productName = row.parseStringOption(6),
      eanCu = row.parseStringOption(7),
      eanDu = row.parseStringOption(8),
      mrdr = row.parseStringOption(9),
      unit = row.parseStringOption(10),
      unitPrice = row.parseBigDecimalOption(11),
      unitPriceOriginal = row.parseStringOption(11),
      unitPriceCurrency = row.parseStringOption(12)
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
        productRecords("countryCode") === countryRecords("countryCode"),
        JoinType.LeftOuter
      )
      .map {
        case (productRecord, countryRecord) ⇒ Option(countryRecord).fold(productRecord) { cr ⇒
          productRecord.copy(
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

    val requiredNrOfColumns = 13
    val productRecords = storage
      .readFromCsv(inputFile, fieldSeparator = csvColumnSeparator)
      .filter { row ⇒
        if (row.length != requiredNrOfColumns) {
          throw new InputMismatchException(
            s"An input CSV row did not have the required $requiredNrOfColumns columns\n${row.toString()}"
          )
          false
        } else {
          true
        }
      }
      .map(rowToProductRecord)

    val countryRecords = storage.createCountries

    val transformed = transform(spark, productRecords, countryRecords)

    storage
      .writeToParquet(transformed, outputFile, partitionBy = Seq("countryCode"))
  }
}
