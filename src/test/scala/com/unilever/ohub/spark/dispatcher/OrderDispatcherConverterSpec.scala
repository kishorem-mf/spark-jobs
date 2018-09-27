package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.dispatcher.model.DispatcherOrder
import com.unilever.ohub.spark.domain.entity.{ Order, OrderLine, TestOrderLines, TestOrders }
import org.apache.spark.sql.Dataset
import cats.syntax.option._

class OrderDispatcherConverterSpec extends SparkJobSpec with TestOrders with TestOrderLines {

  private val orderDispatcherConverter = OrderDispatcherConverter

  describe("order dispatcher converter") {
    it("should convert a domain order correctly into an dispatcher order") {
      import spark.implicits._

      /**
       * Input file containing order records
       */
      val inputOrders: Dataset[Order] = {
        spark.createDataset(
          List(defaultOrder)
        )
      }

      /**
       * Input file containing order line records
       */
      val inputOrderLines: Dataset[OrderLine] = {
        spark.createDataset(
          List(defaultOrderLine)
        )
      }

      /**
       * There is no previous run containing order records, so
       * we create an empty dataset
       */
      val emptyDataset: Dataset[Order] = spark.emptyDataset[Order]

      /**
       * Transformed DispatcherOrders
       */
      val result: List[DispatcherOrder] = {
        orderDispatcherConverter.transform(spark, inputOrders, emptyDataset, inputOrderLines).collect().toList
      }

      result should contain(DispatcherOrder(
        CAMPAIGN_CODE = "UNKNOWN",
        CAMPAIGN_NAME = "campaign",
        COMMENTS = none,
        ORD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        CP_ORIG_INTEGRATION_ID = "some~contact~person",
        COUNTRY_CODE = "country-code",
        WHOLESALER_ID = none,
        WHOLESALER_LOCATION = none,
        WHOLESALER = "Van der Valk",
        WHOLESALER_CUSTOMER_NUMBER = none,
        DELETED_FLAG = false,
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        OPR_ORIG_INTEGRATION_ID = "some~operator~id",
        SOURCE_ID = "source-entity-id",
        SOURCE = "source-name",
        TRANSACTION_DATE = "2015-06-30 13:49:00",
        ORDER_TYPE = "DIRECT",
        VAT = none,
        ORDER_EMAIL_ADDRESS = none,
        ORDER_PHONE_NUMBER = none,
        ORDER_MOBILE_PHONE_NUMBER = none,
        TOTAL_VALUE_ORDER_CURRENCY = none,
        TOTAL_VALUE_ORDER_AMOUNT = none,
        ORIGIN = none,
        WS_DC = none,
        ORDER_UID = none,
        DELIVERY_STREET = none,
        DELIVERY_HOUSE_NUMBER = none,
        DELIVERY_HOUSE_NUMBER_EXT = none,
        DELIVERY_POST_CODE = none,
        DELIVERY_CITY = none,
        DELIVERY_STATE = none,
        DELIVERY_COUNTRY = none,
        DELIVERY_PHONE = none,
        INVOICE_NAME = none,
        INVOICE_STREET = none,
        INVOICE_HOUSE_NUMBER = none,
        INVOICE_HOUSE_NUMBER_EXT = none,
        INVOICE_ZIPCODE = none,
        INVOICE_CITY = none,
        INVOICE_STATE = none,
        INVOICE_COUNTRY = none
      ))
    }
  }
}
