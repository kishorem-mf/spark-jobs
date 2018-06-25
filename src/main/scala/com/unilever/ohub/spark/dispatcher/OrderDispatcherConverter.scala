package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.dispatcher.model.DispatcherOrder
import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.domain.entity.{ Order, OrderLine }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import scopt.OptionParser

case class OrderDispatcherConverterConfig(
    inputFile: String = "path-to-input-file",
    outputFile: String = "path-to-output-file",
    previousIntegrated: Option[String] = None,
    orderLineFile: String = "path-to-order-line-file",
    postgressUrl: String = "postgress-url",
    postgressUsername: String = "postgress-username",
    postgressPassword: String = "postgress-password",
    postgressDB: String = "postgress-db"
) extends SparkJobConfig

case class OrderLineAggregation(orderConcatId: String, curr: String, total: BigDecimal)

trait SparkJobWithOrderAcmConverterConfig extends SparkJob[OrderDispatcherConverterConfig] {
  override private[spark] def defaultConfig = OrderDispatcherConverterConfig()

  override private[spark] def configParser(): OptionParser[OrderDispatcherConverterConfig] =
    new scopt.OptionParser[OrderDispatcherConverterConfig]("ACM converter") {
      head("converts domain entity into ufs entity.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("previousIntegrated") optional () action { (x, c) ⇒
        c.copy(previousIntegrated = Option(x))
      } text "previousIntegrated is a string property"
      opt[String]("orderLineFile") required () action { (x, c) ⇒
        c.copy(orderLineFile = x)
      } text "orderLineFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"
      opt[String]("postgressUrl") required () action { (x, c) ⇒
        c.copy(postgressUrl = x)
      } text "postgressUrl is a string property"
      opt[String]("postgressUsername") required () action { (x, c) ⇒
        c.copy(postgressUsername = x)
      } text "postgressUsername is a string property"
      opt[String]("postgressPassword") required () action { (x, c) ⇒
        c.copy(postgressPassword = x)
      } text "postgressPassword is a string property"
      opt[String]("postgressDB") required () action { (x, c) ⇒
        c.copy(postgressDB = x)
      } text "postgressDB is a string property"

      version("1.0")
      help("help") text "help text"
    }
}

object OrderDispatcherConverter extends SparkJobWithOrderAcmConverterConfig
  with DeltaFunctions {

  def transform(
    spark: SparkSession,
    orders: Dataset[Order],
    previousIntegrated: Dataset[Order],
    orderLines: Dataset[OrderLine]
  ): Dataset[DispatcherOrder] = {
    val dailyOrders = createDispatcherOrders(spark, orders, orderLines)
    val allPreviousOrders = createDispatcherOrders(spark, previousIntegrated, orderLines)

    integrate[DispatcherOrder](spark, dailyOrders, allPreviousOrders, "ORD_INTEGRATION_ID")
  }

  override def run(spark: SparkSession, config: OrderDispatcherConverterConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating orders dispatcher csv file from [${config.inputFile}] and [${config.orderLineFile}] to [${config.outputFile}]")

    val orders = storage.readFromParquet[Order](config.inputFile)
    val orderLines = storage.readFromParquet[OrderLine](config.orderLineFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[Order](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[Order]
    }

    val transformed = transform(spark, orders, previousIntegrated, orderLines)

    storage.writeToSingleCsv(transformed, config.outputFile, EXTRA_WRITE_OPTIONS)
  }

  def createDispatcherOrders(spark: SparkSession, orders: Dataset[Order], orderLines: Dataset[OrderLine]): Dataset[DispatcherOrder] = {
    import spark.implicits._

    val aggs = orderLines
      .groupBy("orderConcatId")
      .agg(
        first($"currency").as("curr"),
        sum($"amount").as("total"))
      .as[OrderLineAggregation]
    orders
      .joinWith(aggs, orders("concatId") === aggs("orderConcatId"), "left")
      .map {
        case (order, agg) ⇒
          if (agg == null) log.warn("no order lines found for order " + order.concatId)
          DispatcherOrder.fromOrder(order)
      }
  }
}
