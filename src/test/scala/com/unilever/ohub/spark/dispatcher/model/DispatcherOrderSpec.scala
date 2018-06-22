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
      // DispatcherOrder(Some(UNKNOWN),Some(campaign),None,country-code~source-name~source-entity-id,Some(some~contact~person),country-code,None,None,Some(Van der Valk),None,false,2015-06-30 13:49:00,2015-06-30 13:49:00,some~operator~id,source-entity-id,source-name,2015-06-30 13:49:00,DIRECT,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None) did not equal DispatcherOrder(None,None,None,,None,,None,None,None,None,false,,,,,,,,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None) (DispatcherOrderSpec.scala:17)
      DispatcherOrder.fromOrder(TEST_ORDER) shouldEqual DispatcherOrder(
        CAMPAIGN_CODE = "UNKNOWN".some,
        CAMPAIGN_NAME = "campaign".some,
        COMMENTS = None,
        ORD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        CP_ORIG_INTEGRATION_ID = "some~contact~person".some,
        COUNTRY_CODE = "country-code",
        WHOLESALER_ID = None,
        WHOLESALER_LOCATION = None,
        WHOLESALER = None,
        WHOLESALER_CUSTOMER_NUMBER = None,
        DELETED_FLAG = false,
        CREATED_AT = "",
        UPDATED_AT = "",
        OPR_ORIG_INTEGRATION_ID = "",
        SOURCE_ID = "",
        SOURCE = "",
        TRANSACTION_DATE = "",
        ORDER_TYPE = "",
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
      )
    }
  }
}
