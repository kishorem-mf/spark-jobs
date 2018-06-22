package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.dispatcher.model.DispatcherProduct
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

object ProductDispatcherConverter extends SparkJob[DefaultWithDbAndDeltaConfig]
  with DeltaFunctions {

  def transform(
    spark: SparkSession,
    products: Dataset[Product],
    previousIntegrated: Dataset[Product]
  ): Dataset[DispatcherProduct] = {
    val dailyProducts = createDispatcherProducts(spark, products)
    val allPreviousProducts = createDispatcherProducts(spark, previousIntegrated)

    integrate[DispatcherProduct](spark, dailyProducts, allPreviousProducts, "PRD_INTEGRATION_ID")
  }

  override private[spark] def defaultConfig = DefaultWithDbAndDeltaConfig()

  override private[spark] def configParser(): OptionParser[DefaultWithDbAndDeltaConfig] = DefaultWithDbAndDeltaConfigParser()

  override def run(spark: SparkSession, config: DefaultWithDbAndDeltaConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating products Dispatcher csv file from [${config.inputFile}] to [${config.outputFile}]")

    val products = storage.readFromParquet[Product](config.inputFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[Product](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[Product]
    }

    val transformed = transform(spark, products, previousIntegrated)

    storage.writeToSingleCsv(
      ds = transformed,
      outputFile = config.outputFile,
      options = EXTRA_WRITE_OPTIONS
    )
  }

  def createDispatcherProducts(
    spark: SparkSession,
    products: Dataset[Product]
  ): Dataset[DispatcherProduct] = {
    import spark.implicits._
    products.map(DispatcherProduct.fromProduct)
  }
}
