package com.unilever.ohub.spark.acm

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJob
import com.unilever.ohub.spark.storage.Storage
import org.apache.spark.sql.{ Dataset, SparkSession }

case class Order(
  ORDER_CONCAT_ID: String,
  COUNTRY_CODE: String,
  ORDER_TYPE: String,
  REF_CONTACT_PERSON_ID: String,
  REF_OPERATOR_ID: String,
  CAMPAIGN_CODE: String,
  CAMPAIGN_NAME: String,
  WHOLESALER: String,
  TRANSACTION_DATE: Timestamp,
  ORDER_VALUE: Double,
  CURRENCY_CODE: String
)

case class UfsOrder(
  ORDER_ID: String,
  COUNTRY_CODE: String,
  ORDER_TYPE: String,
  CP_LNKD_INTEGRATION_ID: String,
  OPR_LNKD_INTEGRATION_ID: String,
  CAMPAIGN_CODE: String,
  CAMPAIGN_NAME: String,
  WHOLESALER: String,
  ORDER_TOKEN: String,
  TRANSACTION_DATE: String,
  ORDER_AMOUNT: Double,
  ORDER_AMOUNT_CURRENCY_CODE: String,
  DELIVERY_STREET: String,
  DELIVERY_HOUSENUMBER: String,
  DELIVERY_ZIPCODE: String,
  DELIVERY_CITY: String,
  DELIVERY_STATE: String,
  DELIVERY_COUNTRY: String,
  DELIVERY_PHONE: String
)

object OrderAcmConverter extends SparkJob {
  def transform(spark: SparkSession, orders: Dataset[Order]): Dataset[UfsOrder] = {
    import spark.implicits._

    val dateFormat = "yyyy-MM-dd HH:mm:ss"

    orders.map(order => UfsOrder(
      ORDER_ID = order.ORDER_CONCAT_ID,
      COUNTRY_CODE = order.COUNTRY_CODE,
      ORDER_TYPE = order.ORDER_TYPE,
      CP_LNKD_INTEGRATION_ID = order.REF_CONTACT_PERSON_ID,
      OPR_LNKD_INTEGRATION_ID = order.REF_OPERATOR_ID,
      CAMPAIGN_CODE = order.CAMPAIGN_CODE,
      CAMPAIGN_NAME = order.CAMPAIGN_NAME,
      WHOLESALER = order.WHOLESALER,
      ORDER_TOKEN = "",
      TRANSACTION_DATE = order.TRANSACTION_DATE.formatted(dateFormat),
      ORDER_AMOUNT = BigDecimal(order.ORDER_VALUE).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
      ORDER_AMOUNT_CURRENCY_CODE = order.CURRENCY_CODE,
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
      .readFromParquet[Order](
        inputFile,
        selectColumns =
          $"ORDER_CONCAT_ID",
          $"COUNTRY_CODE",
          $"ORDER_TYPE",
          $"REF_CONTACT_PERSON_ID",
          $"REF_OPERATOR_ID",
          $"CAMPAIGN_CODE",
          $"CAMPAIGN_NAME",
          $"WHOLESALER",
          $"TRANSACTION_DATE",
          $"ORDER_VALUE",
          $"CURRENCY_CODE"
      )

    val transformed = transform(spark, orders)

    storage
      .writeToCSV(transformed, outputFile)
  }
}
