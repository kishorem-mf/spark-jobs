package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ OrderLine, Product }
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.sql.JoinType
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class OrderLineMergingConfig(
    orderLineInputFile: String = "order-input-file",
    previousIntegrated: String = "previous-integrated-orderlines",
    productsIntegrated: String = "products-input-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

// Technically not really order MERGING, but we need to update foreign key IDs in the other records
object OrderLineMerging extends SparkJob[OrderLineMergingConfig] {

  def transform(
    spark: SparkSession,
    orders: Dataset[OrderLine],
    previousIntegrated: Dataset[OrderLine],
    products: Dataset[Product]
  ): Dataset[OrderLine] = {
    import spark.implicits._

    val allOrderLines = previousIntegrated
      .joinWith(orders, previousIntegrated("concatId") === orders("concatId"), JoinType.FullOuter)
      .map {
        case (integrated, orderLine) ⇒
          if (orderLine == null) {
            integrated
          } else if (integrated == null) {
            orderLine
          } else {
            orderLine.copy(ohubId = integrated.ohubId)
          }
      }

    allOrderLines
      .joinWith(products, $"productConcatId" === products("concatId"), "left")
      .map {
        case (order, product) ⇒
          if (product == null) order
          else order.copy(productOhubId = product.ohubId)
      }
  }

  override private[spark] def defaultConfig = OrderLineMergingConfig()

  override private[spark] def configParser(): OptionParser[OrderLineMergingConfig] =
    new scopt.OptionParser[OrderLineMergingConfig]("Order merging") {
      head("merges orders into an integrated order output file.", "1.0")
      opt[String]("orderLineInputFile") required () action { (x, c) ⇒
        c.copy(orderLineInputFile = x)
      } text "orderLineInputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = x)
      } text "previousIntegrated is a string property"
      opt[String]("productsIntegrated") optional () action { (x, c) ⇒
        c.copy(productsIntegrated = x)
      } text "productsIntegrated is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OrderLineMergingConfig, storage: Storage): Unit = {
    import spark.implicits._

    val orderRecords = storage.readFromParquet[OrderLine](config.orderLineInputFile)
    val previousIntegrated = storage.readFromParquet[OrderLine](config.previousIntegrated)
    val products = storage.readFromParquet[Product](config.productsIntegrated)

    val transformed = transform(spark, orderRecords, previousIntegrated, products)

    storage.writeToParquet(transformed, config.outputFile)
  }
}
