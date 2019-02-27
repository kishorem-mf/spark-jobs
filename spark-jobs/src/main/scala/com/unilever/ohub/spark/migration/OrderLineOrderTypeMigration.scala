package com.unilever.ohub.spark.migration

import com.unilever.ohub.spark.domain.entity.{ Order, OrderLine }
import com.unilever.ohub.spark.sql.JoinType
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class OrderLineOrderTypeMigrationConfig(
    orderLinesIntegratedFile: String = "orderline-input-file",
    ordersIntegratedFile: String = "order-input-file"
) extends SparkJobConfig

/**
 * Sparkjob that merges ordelines with orders to copy the order's type to set at the orderline's orderType field.
 * It overwrites the orderlines with the updated ones!
 */
object OrderLineOrderTypeMigration extends SparkJob[OrderLineOrderTypeMigrationConfig] {
  def transform(
    spark: SparkSession,
    orderLines: Dataset[OrderLine],
    orders: Dataset[Order]
  ): Dataset[OrderLine] = {
    import spark.implicits._

    orderLines.joinWith(orders, orderLines("orderConcatId") === orders("concatId"), JoinType.Left)
      .map {
        case (orderLine, order) ⇒
          if (order != null) {
            orderLine.copy(orderType = Some(order.`type`))
          } else {
            orderLine
          }
      }
  }

  override private[spark] def defaultConfig = OrderLineOrderTypeMigrationConfig()

  override private[spark] def configParser(): OptionParser[OrderLineOrderTypeMigrationConfig] =
    new scopt.OptionParser[OrderLineOrderTypeMigrationConfig]("Order merging") {
      head("merges orders into an integrated order output file.", "1.0")
      opt[String]("orderLinesIntegratedFile") required () action { (x, c) ⇒
        c.copy(orderLinesIntegratedFile = x)
      } text "orderLineInputFile is a string property"
      opt[String]("ordersIntegratedFile") optional () action { (x, c) ⇒
        c.copy(ordersIntegratedFile = x)
      } text "previousIntegrated is a string property"

      version("1.0")
      help("help") text "help text"
    }

  override def run(spark: SparkSession, config: OrderLineOrderTypeMigrationConfig, storage: Storage): Unit = {
    import spark.implicits._

    val orderLinesRecords = storage.readFromParquet[OrderLine](config.orderLinesIntegratedFile)

    if (orderLinesRecords.filter($"orderType".isNull).count() > 0) { // Only convert if there are undefined orderTypes
      val orderRecords = storage.readFromParquet[Order](config.ordersIntegratedFile)
      val transformed = transform(spark, orderLinesRecords, orderRecords)

      storage.writeToParquet(transformed, config.orderLinesIntegratedFile)
    }
  }
}
