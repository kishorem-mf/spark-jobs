package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.{ DefaultConfig, SparkJobWithDefaultConfig }
import com.unilever.ohub.spark.acm.model.UFSProduct
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object ProductAcmConverter extends SparkJobWithDefaultConfig with AcmTransformationFunctions with AcmConverter {

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

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating products ACM csv file from [${config.inputFile}] to [${config.outputFile}]")

    val products = storage.readFromParquet[Product](config.inputFile)

    val transformed = transform(spark, products)

    storage.writeToSingleCsv(transformed, config.outputFile, delim = outputCsvDelimiter, quote = outputCsvQuote)
  }
}
