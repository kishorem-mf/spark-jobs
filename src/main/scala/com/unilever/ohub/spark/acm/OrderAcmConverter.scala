package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.{ SparkJob, SparkJobConfig }
import com.unilever.ohub.spark.acm.model.UFSOrder
import com.unilever.ohub.spark.domain.entity.{ Order, OrderLine }
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }
import scopt.OptionParser

case class OrderAcmConverterConfig(
    inputFile: String = "path-to-input-file",
    orderLineFile: String = "path-to-order-line-file",
    outputFile: String = "path-to-output-file"
) extends SparkJobConfig

trait SparkJobWithOrderAcmConverterConfig extends SparkJob[OrderAcmConverterConfig] {
  override private[spark] def defaultConfig = OrderAcmConverterConfig()

  override private[spark] def configParser(): OptionParser[OrderAcmConverterConfig] =
    new scopt.OptionParser[OrderAcmConverterConfig]("Spark job default") {
      head("run a spark job with default config.", "1.0")
      opt[String]("inputFile") required () action { (x, c) ⇒
        c.copy(inputFile = x)
      } text "inputFile is a string property"
      opt[String]("orderLineFile") required () action { (x, c) ⇒
        c.copy(orderLineFile = x)
      } text "orderLineFile is a string property"
      opt[String]("outputFile") required () action { (x, c) ⇒
        c.copy(outputFile = x)
      } text "outputFile is a string property"

      version("1.0")
      help("help") text "help text"
    }
}

object OrderAcmConverter extends SparkJobWithOrderAcmConverterConfig with AcmConverter {
  private val dateFormat = "yyyy-MM-dd HH:mm:ss"

  def transform(spark: SparkSession, orders: Dataset[Order], orderLines: Dataset[OrderLine]): Dataset[UFSOrder] = {
    import spark.implicits._

    orders.map(order ⇒ {
      val lines = orderLines.filter(_.orderConcatId == order.concatId)
      UFSOrder(
        ORDER_ID = order.concatId,
        COUNTRY_CODE = Some(order.countryCode),
        ORDER_TYPE = order.`type`,
        CP_LNKD_INTEGRATION_ID = order.contactPersonOhubId,
        OPR_LNKD_INTEGRATION_ID = order.operatorOhubId,
        CAMPAIGN_CODE = order.campaignCode,
        CAMPAIGN_NAME = order.campaignName,
        WHOLESALER = order.distributorId,
        ORDER_TOKEN = "",
        TRANSACTION_DATE = order.transactionDate.map(_.formatted(dateFormat)),
        ORDER_AMOUNT = Some(
          lines
            .map(_.amount.getOrElse(BigDecimal(0)))
            .reduce((a, b) ⇒ a + b)
            .toDouble
        ),
        ORDER_AMOUNT_CURRENCY_CODE = lines.first.currency,
        DELIVERY_STREET = "",
        DELIVERY_HOUSENUMBER = "",
        DELIVERY_ZIPCODE = "",
        DELIVERY_CITY = "",
        DELIVERY_STATE = "",
        DELIVERY_COUNTRY = "",
        DELIVERY_PHONE = ""
      )
    })
  }

  override def run(spark: SparkSession, config: OrderAcmConverterConfig, storage: Storage): Unit = {
    import spark.implicits._

    log.info(s"Generating orders ACM csv file from [${config.inputFile}] and [${config.orderLineFile}] to [${config.outputFile}]")

    val orders = storage.readFromParquet[Order](config.inputFile)
    val orderLines = storage.readFromParquet[OrderLine](config.orderLineFile)
    val transformed = transform(spark, orders, orderLines)

    storage.writeToSingleCsv(transformed, config.outputFile, delim = outputCsvDelimiter, quote = outputCsvQuote)
  }
}
