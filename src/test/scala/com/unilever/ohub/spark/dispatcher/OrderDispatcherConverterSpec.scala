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
        COMMENTS = None,
        ORD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        CP_ORIG_INTEGRATION_ID = "some~contact~person".some,
        COUNTRY_CODE = "country-code",
        WHOLESALER_ID = None,
        WHOLESALER_LOCATION = None,
        WHOLESALER = "Van der Valk".some,
        WHOLESALER_CUSTOMER_NUMBER = None,
        DELETED_FLAG = false,
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        OPR_ORIG_INTEGRATION_ID = "some~operator~id",
        SOURCE_ID = "source-entity-id",
        SOURCE = "source-name",
        TRANSACTION_DATE = "2015-06-30 13:49:00",
        ORDER_TYPE = "DIRECT",
        VAT = None,
        ORDER_EMAIL_ADDRESS = None,
        ORDER_PHONE_NUMBER = None,
        ORDER_MOBILE_PHONE_NUMBER = None,
        TOTAL_VALUE_ORDER_CURRENCY = None,
        TOTAL_VALUE_ORDER_AMOUNT = None,
        ORIGIN = None,
        WS_DC = None,
        ORDER_UID = None,
        DELIVERY_STREET = None,
        DELIVERY_HOUSE_NUMBER = None,
        DELIVERY_HOUSE_NUMBER_EXT = None,
        DELIVERY_POST_CODE = None,
        DELIVERY_CITY = None,
        DELIVERY_STATE = None,
        DELIVERY_COUNTRY = None,
        DELIVERY_PHONE = None,
        INVOICE_NAME = None,
        INVOICE_STREET = None,
        INVOICE_HOUSE_NUMBER = None,
        INVOICE_HOUSE_NUMBER_EXT = None,
        INVOICE_ZIPCODE = None,
        INVOICE_CITY = None,
        INVOICE_STATE = None,
        INVOICE_COUNTRY = None
      ))
    }
  }
}
