package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.ProductRecord
import com.unilever.ohub.spark.acm.model.UFSProduct
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object ProductAcmConverter extends SparkJob {
  private val dateFormat = "yyyy-MM-dd HH:mm:ss"

  def transform(spark: SparkSession, products: Dataset[ProductRecord]): Dataset[UFSProduct] = {
    import spark.implicits._

    products.map { product ⇒
      UFSProduct(
        COUNTY_CODE = product.countryCode,
        PRODUCT_NAME = product.productName,
        PRD_INTEGRATION_ID = product.productConcatId,
        EAN_CODE = product.eanCu,
        MRDR_CODE = product.mrdr,
        CREATED_AT = Some(product.dateCreated.formatted(dateFormat)),
        UPDATED_AT = Some(product.dateCreated.formatted(dateFormat)),
        DELETE_FLAG = product.status.map(status ⇒ if (status) "N" else "Y")
      )
    }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating products ACM csv file from [$inputFile] to [$outputFile]")

    val products = storage
      .readFromParquet[ProductRecord](inputFile)

    val transformed = transform(spark, products)

    storage
      .writeToCsv(transformed, outputFile, partitionBy = Seq("COUNTY_CODE"))
  }
}
