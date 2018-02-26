package com.unilever.ohub.spark.acm

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.data.ufs.UFSOrder
import com.unilever.ohub.spark.data.OrderRecord
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

object OrderAcmConverter extends SparkJob {
  private val dateFormat = "yyyy-MM-dd HH:mm:ss"

  def transform(spark: SparkSession, orders: Dataset[OrderRecord]): Dataset[UFSOrder] = {
    import spark.implicits._

    orders.map(order => UFSOrder(
      ORDER_ID = order.orderConcatId,
      COUNTRY_CODE = order.countryCode,
      ORDER_TYPE = order.orderType,
      CP_LNKD_INTEGRATION_ID = order.refContactPersonId,
      OPR_LNKD_INTEGRATION_ID = order.refOperatorId,
      CAMPAIGN_CODE = order.campaignCode,
      CAMPAIGN_NAME = order.campaignName,
      WHOLESALER = order.wholesaler,
      ORDER_TOKEN = "",
      TRANSACTION_DATE = order.transactionDate.map(_.formatted(dateFormat)),
      ORDER_AMOUNT = order.orderValue.map(_.setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble),
      ORDER_AMOUNT_CURRENCY_CODE = order.currencyCode,
      DELIVERY_STREET = "",
      DELIVERY_HOUSENUMBER = "",
      DELIVERY_ZIPCODE = "",
      DELIVERY_CITY = "",
      DELIVERY_STATE = "",
      DELIVERY_COUNTRY = "",
      DELIVERY_PHONE = ""
    ))
  }

  override val neededFilePaths = Array("INPUT_FILE", "OUTPUT_FILE")

  override def run(spark: SparkSession, filePaths: scala.Product, storage: Storage): Unit = {
    import spark.implicits._

    val (inputFile: String, outputFile: String) = filePaths

    log.info(s"Generating orders ACM csv file from [$inputFile] to [$outputFile]")

    val orders = storage
      .readFromParquet[OrderRecord](inputFile)

    val transformed = transform(spark, orders)

    storage
      .writeToCSV(transformed, outputFile)
  }
}
