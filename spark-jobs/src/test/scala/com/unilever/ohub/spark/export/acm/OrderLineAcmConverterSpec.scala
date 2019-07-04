package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.TestOrderLines
import com.unilever.ohub.spark.export.acm.model.AcmOrderLine
import org.scalatest.{FunSpec, Matchers}

class OrderLineAcmConverterSpec extends FunSpec with TestOrderLines with Matchers {

  private[acm] val SUT = OrderLineAcmConverter

  describe("OrderLine acm converter") {
    it("should convert a order line correctly into an acm order line") {
      val result = SUT.convert(defaultOrderLine)
      val expectedAcmOrderLine = AcmOrderLine(
        ORDERLINE_ID = "country-code~source-name~source-entity-id",
        ORD_INTEGRATION_ID = "",
        QUANTITY = "0",
        AMOUNT = "0.00",
        LOYALTY_POINTS = "123.00",
        PRD_INTEGRATION_ID = "product-ohub-id",
        SAMPLE_ID = "",
        CAMPAIGN_LABEL = "campaign-label",
        COMMENTS = "",
        DELETED_FLAG = "N"
      )

      result shouldBe expectedAcmOrderLine
    }
  }
}
