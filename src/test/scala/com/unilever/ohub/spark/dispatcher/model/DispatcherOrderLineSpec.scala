package com.unilever.ohub.spark.dispatcher
package model

import com.unilever.ohub.spark.SimpleSpec
import com.unilever.ohub.spark.domain.entity.TestOrderLines

class DispatcherOrderLineSpec extends SimpleSpec {
  final val TEST_ORDER_LINE = {
    TestOrderLines.defaultOrderLine
      .copy(isGoldenRecord = false)
      .copy(isActive = false)
      .copy(ohubId = Some("randomId"))
      .copy(orderConcatId = "order-concat-id")
  }

  describe("DispatcherContactPerson") {
    it("should map from a ContactPerson") {
      DispatcherOrderLine.fromOrderLine(TEST_ORDER_LINE) shouldEqual DispatcherOrderLine(
        AMOUNT = BigDecimal(0),
        CAMPAIGN_LABEL = "campaign-label".some,
        COMMENTS = Option.empty,
        ODL_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        COUNTRY_CODE = "country-code",
        UNIT_PRICE_CURRENCY = Option.empty,
        DELETE_FLAG = true,
        LOYALTY_POINTS = 123L.some,
        ODS_CREATED = "2015-06-30 13:49:00",
        ODS_UPDATED = "2015-06-30 13:49:00",
        ORD_INTEGRATION_ID = "order-concat-id",
        UNIT_PRICE = Option.empty,
        PRD_INTEGRATION_ID = "product-concat-id",
        QUANTITY = 0L,
        SOURCE = "source-name"
      )
    }
  }
}
