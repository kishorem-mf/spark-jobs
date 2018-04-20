package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.acm.model.UFSProduct
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object ProductAcmConverter extends SparkJob with AcmTransformationFunctions {

  def transform(spark: SparkSession, products: Dataset[Product]): Dataset[UFSProduct] = {
    import spark.implicits._

    products.map { product ⇒
      UFSProduct(
        COUNTY_CODE = Some(product.countryCode),
        PRODUCT_NAME = Some(product.name),
        PRD_INTEGRATION_ID = product.concatId,
        EAN_CODE = product.eanConsumerUnit,
        MRDR_CODE = product.code,
        CREATED_AT = product.dateCreated.map(formatWithPattern()),
        UPDATED_AT = product.dateUpdated.map(formatWithPattern()),
        DELETE_FLAG = Some(if (product.isActive) "N" else "Y")
      )
    }
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths
    log.info(s"Generating products ACM csv file from [$inputFile] to [$outputFile]")

    val products = storage.readFromParquet[Product](inputFile)

    val transformed = transform(spark, products)

    storage.writeToCsv(transformed, outputFile, partitionBy = Seq("COUNTY_CODE"))
  }
}
