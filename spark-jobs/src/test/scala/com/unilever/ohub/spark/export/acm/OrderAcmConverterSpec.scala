package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.TestOrders
import com.unilever.ohub.spark.export.acm.model.AcmOrder
import org.scalatest.{FunSpec, Matchers}

class OrderAcmConverterSpec extends FunSpec with TestOrders with Matchers {

  private[acm] val SUT = OrderAcmConverter

  describe("Order acm converter") {
    it("should convert an order into an acm order") {

      val result = SUT.convert(defaultOrder.copy(operatorOhubId = Some("operator-ohub-id"), contactPersonOhubId = Some("contactperson-ohub-id")))

      val expectedAcmOrder = AcmOrder(
        ORDER_ID = "country-code~source-name~source-entity-id",
        REF_ORDER_ID = "source-entity-id",
        COUNTRY_CODE = "country-code",
        ORDER_TYPE = "DIRECT",
        CP_LNKD_INTEGRATION_ID = "contactperson-ohub-id",
        OPR_LNKD_INTEGRATION_ID = ("operator-ohub-id"),
        CAMPAIGN_CODE = ("UNKNOWN"),
        CAMPAIGN_NAME = ("campaign"),
        WHOLESALER = "Van der Valk",
        WHOLESALER_ID = "",
        WHOLESALER_CUSTOMER_NUMBER = "",
        WHOLESALER_LOCATION = "",
        TRANSACTION_DATE = ("2015/06/30 13:49:00"),
        DELIVERY_STREET = ("deliveryOperatorStreet"),
        DELIVERY_HOUSENUMBER = ("deliveryOperatorHouseNumber"),
        DELIVERY_ZIPCODE = ("deliveryOperatorZipCode"),
        DELIVERY_CITY = ("deliveryOperatorCity"),
        DELIVERY_STATE = ("deliveryOperatorState"),
        DELIVERY_COUNTRY = ("deliveryOperatorCountry"),
        INVOICE_NAME = ("invoiceOperatorName"),
        INVOICE_STREET = ("invoiceOperatorStreet"),
        INVOICE_HOUSE_NUMBER = ("invoiceOperatorHouseNumber"),
        INVOICE_HOUSE_NUMBER_EXT = ("invoiceOperatorHouseNumberExtension"),
        INVOICE_ZIPCODE = ("invoiceOperatorZipCode"),
        INVOICE_CITY = ("invoiceOperatorCity"),
        INVOICE_STATE = ("invoiceOperatorState"),
        INVOICE_COUNTRY = ("invoiceOperatorCountry"),
        COMMENTS = "",
        VAT = "",
        ORDER_AMOUNT = "10.00",
        DELETED_FLAG = "N",
        ORDER_AMOUNT_CURRENCY_CODE = ("EUR")
      )
      result shouldBe expectedAcmOrder
    }
  }
}
