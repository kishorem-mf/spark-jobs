package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOrders
import com.unilever.ohub.spark.export.dispatch.model.DispatchOrder

class OrderDispatchConverterSpec extends SparkJobSpec with TestOrders {

  val SUT = OrderDispatchConverter

  describe("Order dispatch converter") {
    it("should convert an order into an dispatch order") {

      val result = SUT.convert(defaultOrder)

      val expectedOrder = DispatchOrder(
        SOURCE_ID = "source-entity-id",
        ORD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        COUNTRY_CODE = "country-code",
        SOURCE = "source-name",
        DELETED_FLAG = "N",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        ORDER_TYPE = "DIRECT",
        CP_ORIG_INTEGRATION_ID = "some~contact~person",
        OPR_ORIG_INTEGRATION_ID = "some~operator~id",
        WHOLESALER = "Van der Valk",
        WHOLESALER_ID = "",
        WHOLESALER_CUSTOMER_NUMBER = "",
        WHOLESALER_LOCATION = "",
        TRANSACTION_DATE = "2015-06-30 13:49:00",
        ORDER_UID = "",
        CAMPAIGN_CODE = "UNKNOWN",
        CAMPAIGN_NAME = "campaign",
        DELIVERY_STREET = "deliveryOperatorStreet",
        DELIVERY_HOUSE_NUMBER = "deliveryOperatorHouseNumber",
        DELIVERY_HOUSE_NUMBER_EXT = "deliveryOperatorHouseNumberExtension",
        DELIVERY_POST_CODE = "deliveryOperatorZipCode",
        DELIVERY_CITY = "deliveryOperatorCity",
        DELIVERY_STATE = "deliveryOperatorState",
        DELIVERY_COUNTRY = "deliveryOperatorCountry",
        UFS_CLIENT_NUMBER = "C1234",
        DELIVERY_TYPE = "DELIVERY",
        PREFERRED_DELIVERY_DATE_OPTION = "DATE",
        PREFERRED_DELIVERY_DATE = "2020-02-28 00:00:00",
        INVOICE_NAME = "invoiceOperatorName",
        INVOICE_STREET = "invoiceOperatorStreet",
        INVOICE_HOUSE_NUMBER = "invoiceOperatorHouseNumber",
        INVOICE_HOUSE_NUMBER_EXT = "invoiceOperatorHouseNumberExtension",
        INVOICE_ZIPCODE = "invoiceOperatorZipCode",
        INVOICE_CITY = "invoiceOperatorCity",
        INVOICE_STATE = "invoiceOperatorState",
        INVOICE_COUNTRY = "invoiceOperatorCountry",
        COMMENTS = "",
        VAT = "",
        TOTAL_VALUE_ORDER_AMOUNT = "10.00",
        TOTAL_VALUE_ORDER_CURRENCY = "EUR"
      )

      result shouldBe expectedOrder
    }
  }
}
