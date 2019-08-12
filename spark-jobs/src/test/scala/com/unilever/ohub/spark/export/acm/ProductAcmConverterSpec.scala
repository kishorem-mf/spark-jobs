package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.domain.entity.TestProducts
import com.unilever.ohub.spark.export.acm.model.AcmProduct
import org.scalatest.{FunSpec, Matchers}

class ProductAcmConverterSpec extends FunSpec with TestProducts with Matchers {

  private[acm] val SUT = ProductAcmConverter

  describe("Product acm converter") {
    it("should convert a product correctly into an acm product") {
      val product = defaultProduct.copy(eanConsumerUnit = Some("1234"), code = Some("4567"))
      val result = SUT.convert(product)

      val expectedAcmOrderLine = AcmProduct(
        COUNTY_CODE = "country-code",
        PRODUCT_NAME = "product-name",
        PRD_INTEGRATION_ID = "ohub-id",
        EAN_CODE = "1234",
        MRDR_CODE = "4567",
        CREATED_AT = "2015/06/30 13:49:00",
        UPDATED_AT = "2015/06/30 13:49:00",
        DELETE_FLAG = "N"
      )

      result shouldBe expectedAcmOrderLine
    }
  }
}
