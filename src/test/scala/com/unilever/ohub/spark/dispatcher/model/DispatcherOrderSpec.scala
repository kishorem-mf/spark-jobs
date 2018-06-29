package com.unilever.ohub.spark.dispatcher
package model

import com.unilever.ohub.spark.SimpleSpec
import com.unilever.ohub.spark.domain.entity.TestOrders
import cats.syntax.option._

class DispatcherOrderSpec extends SimpleSpec {

  final val TEST_ORDER = {
    TestOrders
      .defaultOrder
  }

  describe("DispatcherOrder") {
    it("should map a domain Order") {
      DispatcherOrder.fromOrder(TEST_ORDER) shouldEqual DispatcherOrder(
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
      )
    }
  }
}
