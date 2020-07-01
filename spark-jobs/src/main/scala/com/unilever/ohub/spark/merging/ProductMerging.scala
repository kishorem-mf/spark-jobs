package com.unilever.ohub.spark.merging

import java.util.UUID

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.{SparkJob, SparkJobConfig}
import com.unilever.ohub.spark.domain.entity.{Product, ProductSifu}
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import scopt.OptionParser

case class ProductMergingConfig(
    products: String = "product-input-file",
    products_sifu: String = "product-sifu-input-file",
    previousIntegrated: String = "previous-integrated-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

object ProductMerging extends SparkJob[ProductMergingConfig] {

  def transform(
                 spark: SparkSession,
                 products: Dataset[Product],
                 previousIntegrated: Dataset[Product]
               ): Dataset[Product] = {
    import spark.implicits._

    previousIntegrated
      .joinWith(products, previousIntegrated("concatId") === products("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, product) ⇒
          if (product == null) {
            integrated
          } else {
            val ohubId = if (integrated == null) Some(UUID.randomUUID().toString) else integrated.ohubId

            product.copy(ohubId = ohubId, isGoldenRecord = true)
          }
      }
  }

  override private[spark] def defaultConfig = ProductMergingConfig()

  override private[spark] def configParser(): OptionParser[ProductMergingConfig] =
    new scopt.OptionParser[ProductMergingConfig]("Product merging") {
      head("merges products into an integrated products output file.", "1.0")
      opt[String]("productsInputFile") required() action { (x, c) ⇒
        c.copy(products = x)
      } text "productsInputFile is a string property"
      opt[String]("previousIntegrated") required() action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("outputFile") required() action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: ProductMergingConfig, storage: Storage): Unit = {
    log.info(
      s"Merging products from [${config.products}] and [${config.previousIntegrated}] to [${config.outputFile}]"
    )
    def enrich(
                spark: SparkSession,
                products: Dataset[Product],
                location: String
              ): Dataset[Product] = {
      import spark.implicits._
      val products_sifu = spark.read.parquet(location)
      products
    }

    val products = storage.readFromParquet[Product](config.products)
    val previousIntegrated = storage.readFromParquet[Product](config.previousIntegrated)
    val enriched_products = enrich(spark,products, config.products_sifu)
    val transformed = transform(spark, enriched_products, previousIntegrated)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
