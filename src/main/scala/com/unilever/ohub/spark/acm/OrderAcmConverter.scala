package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.export.DeltaFunctions
import com.unilever.ohub.spark.acm.model.AcmOrder
import com.unilever.ohub.spark.domain.entity.{ Order, OrderLine }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._
import scopt.OptionParser

case class OrderAcmConverterConfig(
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

trait SparkJobWithOrderAcmConverterConfig extends SparkJob[OrderAcmConverterConfig] {
  override private[spark] def defaultConfig = OrderAcmConverterConfig()

  override private[spark] def configParser(): OptionParser[OrderAcmConverterConfig] =
    new scopt.OptionParser[OrderAcmConverterConfig]("ACM converter") {
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

object OrderAcmConverter extends SparkJobWithOrderAcmConverterConfig
  with DeltaFunctions with AcmTransformationFunctions with AcmConverter {

  def transform(
    spark: SparkSession,
    orders: Dataset[Order],
    previousIntegrated: Dataset[Order],
    orderLines: Dataset[OrderLine]
  ): Dataset[AcmOrder] = {
    val dailyAcmOrders = createAcmOrders(spark, orders, orderLines)
    val allPreviousAcmOrders = createAcmOrders(spark, previousIntegrated, orderLines)

    integrate[AcmOrder](spark, dailyAcmOrders, allPreviousAcmOrders, "ORDER_ID")
  }

  override def run(spark: SparkSession, config: OrderAcmConverterConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating orders ACM csv file from [${config.inputFile}] and [${config.orderLineFile}] to [${config.outputFile}]")

    val orders = storage.readFromParquet[Order](config.inputFile)
    val orderLines = storage.readFromParquet[OrderLine](config.orderLineFile)
    val previousIntegrated = config.previousIntegrated match {
      case Some(s) ⇒ storage.readFromParquet[Order](s)
      case None ⇒
        log.warn(s"No existing integrated file specified -- regarding as initial load.")
        spark.emptyDataset[Order]
    }

    val transformed = transform(spark, orders, previousIntegrated, orderLines)

    storage.writeToSingleCsv(transformed, config.outputFile, extraWriteOptions)
  }

  def createAcmOrders(spark: SparkSession, orders: Dataset[Order], orderLines: Dataset[OrderLine]): Dataset[AcmOrder] = {
    import spark.implicits._

    val aggs = orderLines
      .groupBy("orderConcatId")
      .agg(
        first($"currency").as("curr"),
        sum($"amount").as("total"))
      .as[OrderLineAggregation]

    orders.joinWith(aggs, orders("concatId") === aggs("orderConcatId"), "left")
      .map {
        case (order, agg) ⇒ {
          if (agg == null) {
            log.warn("no order lines found for order " + order.concatId)
          }
          AcmOrder(
            ORDER_ID = order.concatId,
            REF_ORDER_ID = order.ohubId,
            COUNTRY_CODE = order.countryCode,
            ORDER_TYPE = order.`type`,
            CP_LNKD_INTEGRATION_ID = order.contactPersonOhubId,
            OPR_LNKD_INTEGRATION_ID = order.operatorConcatId,
            CAMPAIGN_CODE = order.campaignCode,
            CAMPAIGN_NAME = order.campaignName,
            WHOLESALER = order.distributorId,
            WHOLESALER_ID = None,
            WHOLESALER_CUSTOMER_NUMBER = None,
            WHOLESALER_LOCATION = None,
            ORDER_TOKEN = None,
            ORDER_EMAIL_ADDRESS = None,
            ORDER_PHONE_NUMBER = None,
            ORDER_MOBILE_PHONE_NUMBER = None,
            TRANSACTION_DATE = formatWithPattern()(order.transactionDate),
            ORDER_AMOUNT = Option(agg).map(_.total).getOrElse(BigDecimal(0)),
            ORDER_AMOUNT_CURRENCY_CODE = Option(agg).map(_.curr).getOrElse(""),
            DELIVERY_STREET = "",
            DELIVERY_HOUSENUMBER = "",
            DELIVERY_ZIPCODE = "",
            DELIVERY_CITY = "",
            DELIVERY_STATE = "",
            DELIVERY_COUNTRY = "",
            DELIVERY_PHONE = "",
            INVOICE_NAME = None,
            INVOICE_STREET = None,
            INVOICE_HOUSE_NUMBER = None,
            INVOICE_HOUSE_NUMBER_EXT = None,
            INVOICE_ZIPCODE = None,
            INVOICE_CITY = None,
            INVOICE_STATE = None,
            INVOICE_COUNTRY = None,
            COMMENTS = order.comment,
            VAT = order.vat.map(_.toString),
            DELETED_FLAG = boolAsString(!order.isActive)
          )
        }
      }
  }

}
