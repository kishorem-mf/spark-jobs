package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOrderLines
import com.unilever.ohub.spark.export.dispatch.model.DispatchOrderLine

class OrderLineDispatchConverterSpec extends SparkJobSpec with TestOrderLines {

  val SUT = OrderLineDispatchConverter

  describe("Order line dispatch converter") {
    it("should convert an order line into an dispatch order line") {

      val result = SUT.convert(defaultOrderLine)

      val expectedOrder = DispatchOrderLine(
        COUNTRY_CODE = "country-code",
        SOURCE = "source-name",
        DELETE_FLAG = "N",
        ORD_INTEGRATION_ID = "",
        ODL_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        PRD_INTEGRATION_ID = "product-concat-id",
        CAMPAIGN_LABEL = "campaign-label",
        COMMENTS = "",
        QUANTITY = "0",
        AMOUNT = "0.00",
        LOYALTY_POINTS = "123.00",
        UNIT_PRICE = "",
        UNIT_PRICE_CURRENCY = "",
        ODS_CREATED = "2015-06-30 13:49:00",
        ODS_UPDATED = "2015-06-30 13:49:00"
      )

      result shouldBe expectedOrder
    }
  }
}
