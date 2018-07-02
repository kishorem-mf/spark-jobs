package com.unilever.ohub.spark.dispatcher
package model

import com.unilever.ohub.spark.SimpleSpec
import com.unilever.ohub.spark.domain.entity.TestOrders

class DispatcherOrderSpec extends SimpleSpec {

  final val TEST_ORDER = {
    TestOrders
      .defaultOrder
  }

  describe("DispatcherOrder") {
    it("should map a domain Order") {
      DispatcherOrder.fromOrder(TEST_ORDER) shouldEqual DispatcherOrder(
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
      )
    }
  }
}
