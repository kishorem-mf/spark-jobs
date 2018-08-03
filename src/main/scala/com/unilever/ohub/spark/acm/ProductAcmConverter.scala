package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.acm.model.AcmProduct
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object ProductAcmConverter extends SparkJob[DefaultWithDeltaConfig]
  with DeltaFunctions with AcmTransformationFunctions with AcmConverter {

  def transform(
    spark: SparkSession,
    products: Dataset[Product],
    previousIntegrated: Dataset[Product]
  ): Dataset[AcmProduct] = {
    val dailyAcmProducts = createAcmProducts(spark, products)
    val allPreviousAcmProducts = createAcmProducts(spark, previousIntegrated)

    integrate[AcmProduct](spark, dailyAcmProducts, allPreviousAcmProducts, "PRD_INTEGRATION_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDeltaConfig] = DefaultWithDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating products ACM csv file from [${config.inputFile}] to [${config.outputFile}]")

    val products = storage.readFromParquet[Product](config.inputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[Product](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[Product]
    }

    val transformed = transform(spark, products, previousIntegrated)

    storage.writeToSingleCsv(transformed, config.outputFile, extraWriteOptions)(log)
  }

  def createAcmProducts(spark: SparkSession, products: Dataset[Product]): Dataset[AcmProduct] = {
    import spark.implicits._

    products.map { product ⇒
      AcmProduct(
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
}
