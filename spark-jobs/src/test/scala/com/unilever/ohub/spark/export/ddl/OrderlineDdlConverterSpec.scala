package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOrderLines
import com.unilever.ohub.spark.export.ddl.model.DdlOrderline

class OrderlineDdlConverterSpec extends SparkJobSpec with TestOrderLines {
  val SUT = OrderlineDdlConverter
  val orderlineToConvert = defaultOrderLine.copy(ohubId = Some("12345"),
    pricePerUnit = Some(BigDecimal("10")), orderConcatId = "1~3~4")

  describe("Orderline ddl converter") {
    it("should convert a Orderline parquet correctly into an Orderline csv") {
      var result = SUT.convert(orderlineToConvert)
      var expectedOrderline = DdlOrderline(
        Id = "source-entity-id",
        Quantity = "0",
        `List Price` = "10.00",
      `Sales Price` = "",
      `Product Id` = "product-source-entity-id",
        OpportunityId = "4",
        FOC = "",
        `Unit Of Measure` = "",
        `Total Price` = "0.00",
        Discount = "",
        `Discount%` = ""
      )
      result shouldBe expectedOrderline
    }
  }
}
