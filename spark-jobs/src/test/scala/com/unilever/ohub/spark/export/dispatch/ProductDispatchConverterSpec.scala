package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestProducts
import com.unilever.ohub.spark.export.dispatch.model.DispatchProduct

class ProductDispatchConverterSpec extends SparkJobSpec with TestProducts {

  val SUT = ProductDispatchConverter

  describe("Products dispatch converter") {
    it("should convert a product into an dispatch products") {
      val result = SUT.convert(defaultProduct)
      print(result)
      val expectedProduct = DispatchProduct(
        COUNTRY_CODE = "country-code",
        SOURCE = "source-name",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        PRODUCT_NAME = "product-name",
        EAN_CODE = "",
        DELETE_FLAG = "N",
        MRDR_CODE = "",
        PRD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        EAN_CODE_DISPATCH_UNIT = "",
        CATEGORY = "",
        SUB_CATEGORY = "",
        BRAND = "",
        SUB_BRAND = "",
        ITEM_TYPE = "",
        UNIT = "",
        UNIT_PRICE_CURRENCY = "",
        UNIT_PRICE = ""
      )

      result shouldBe expectedProduct
    }
  }
}
