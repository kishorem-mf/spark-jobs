package com.unilever.ohub.spark.acm

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

case class Product(
  COUNTRY_CODE: String,
  PRODUCT_NAME: String,
  PRODUCT_CONCAT_ID: String,
  EAN_CU: String,
  MRDR: String,
  DATE_CREATED: Timestamp,
  DATE_MODIFIED: Timestamp,
  STATUS: Boolean
)

case class UfsProduct(
  // Deliberate misspelling as the consuming system requires it :'(
  COUNTY_CODE: String,
  PRODUCT_NAME: String,
  PRD_INTEGRATION_ID: String,
  EAN_CODE: String,
  MRDR_CODE: String,
  CREATED_AT: String,
  UPDATED_AT: String,
  DELETE_FLAG: String
)

object ProductAcmConverter extends SparkJob {
  def transform(spark: SparkSession, products: Dataset[Product]): Dataset[UfsProduct] = {
    import spark.implicits._

    val dateFormat = "yyyy-MM-dd HH:mm:ss"

    products.map { product =>
      UfsProduct(
        COUNTY_CODE = product.COUNTRY_CODE,
        PRODUCT_NAME = product.PRODUCT_NAME,
        PRD_INTEGRATION_ID = product.PRODUCT_CONCAT_ID,
        EAN_CODE = product.EAN_CU,
        MRDR_CODE = product.MRDR,
        CREATED_AT = product.DATE_CREATED.formatted(dateFormat),
        UPDATED_AT = product.DATE_MODIFIED.formatted(dateFormat),
        DELETE_FLAG = if (product.STATUS) "N" else "Y"
      )
    }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating products ACM csv file from [$inputFile] to [$outputFile]")

    val products = storage
      .readFromParquet[Product](
        inputFile,
        selectColumns =
          $"COUNTRY_CODE",
          $"PRODUCT_NAME",
          $"PRODUCT_CONCAT_ID",
          $"EAN_CU",
          $"MRDR",
          $"DATE_CREATED",
          $"DATE_MODIFIED",
          $"STATUS"
    )

    val transformed = transform(spark, products)

    storage
      .writeToCSV(transformed, outputFile)
  }
}
