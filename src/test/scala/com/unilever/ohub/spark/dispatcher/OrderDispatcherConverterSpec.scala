package com.unilever.ohub.spark.dispatcher

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.dispatcher.model.DispatcherOrder
import com.unilever.ohub.spark.domain.entity.{ Order, OrderLine, TestOrderLines, TestOrders }
import org.apache.spark.sql.Dataset

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
        CAMPAIGN_CODE = "UNKNOWN".some,
        CAMPAIGN_NAME = "campaign".some,
        COMMENTS = Option.empty,
        ORD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        CP_ORIG_INTEGRATION_ID = "some~contact~person".some,
        COUNTRY_CODE = "country-code",
        WHOLESALER_ID = Option.empty,
        WHOLESALER_LOCATION = Option.empty,
        WHOLESALER = "Van der Valk".some,
        WHOLESALER_CUSTOMER_NUMBER = Option.empty,
        DELETED_FLAG = false,
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        OPR_ORIG_INTEGRATION_ID = "some~operator~id",
        SOURCE_ID = "source-entity-id",
        SOURCE = "source-name",
        TRANSACTION_DATE = "2015-06-30 13:49:00",
        ORDER_TYPE = "DIRECT",
        VAT = Option.empty,
        ORDER_EMAIL_ADDRESS = Option.empty,
        ORDER_PHONE_NUMBER = Option.empty,
        ORDER_MOBILE_PHONE_NUMBER = Option.empty,
        TOTAL_VALUE_ORDER_CURRENCY = Option.empty,
        TOTAL_VALUE_ORDER_AMOUNT = Option.empty,
        ORIGIN = Option.empty,
        WS_DC = Option.empty,
        ORDER_UID = Option.empty,
        DELIVERY_STREET = Option.empty,
        DELIVERY_HOUSE_NUMBER = Option.empty,
        DELIVERY_HOUSE_NUMBER_EXT = Option.empty,
        DELIVERY_POST_CODE = Option.empty,
        DELIVERY_CITY = Option.empty,
        DELIVERY_STATE = Option.empty,
        DELIVERY_COUNTRY = Option.empty,
        DELIVERY_PHONE = Option.empty,
        INVOICE_NAME = Option.empty,
        INVOICE_STREET = Option.empty,
        INVOICE_HOUSE_NUMBER = Option.empty,
        INVOICE_HOUSE_NUMBER_EXT = Option.empty,
        INVOICE_ZIPCODE = Option.empty,
        INVOICE_CITY = Option.empty,
        INVOICE_STATE = Option.empty,
        INVOICE_COUNTRY = Option.empty
      ))
    }
  }
}
