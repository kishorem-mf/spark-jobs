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
        id = "source-entity-id",
        quantity = "0",
        listPrice = "10.00",
        salesPrice = "",
        productId = "product-source-entity-id",
        opportunityId = "4",
        foc = "",
        unitOfMeasure = "",
        totalPrice = "0.00",
        discount = "",
        discountPercentage = ""
      )
      result shouldBe expectedOrderline
    }
  }
}
