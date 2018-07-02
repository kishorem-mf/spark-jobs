package com.unilever.ohub.spark.dispatcher
package model

import com.unilever.ohub.spark.SimpleSpec
import com.unilever.ohub.spark.domain.entity.TestProducts

class DispatcherProductSpec extends SimpleSpec {

  final val BIG_DECIMAL = BigDecimal(125.256)
  final val FORMATTED_BIG_DECIMAL = "125.26"
  final val TEST_PRODUCT = {
    TestProducts
      .defaultProduct
      .copy(unitPrice = Option(BIG_DECIMAL))
  }

  describe("DispatcherProduct") {
    it("should map a domain Product") {
      DispatcherProduct.fromProduct(TEST_PRODUCT) shouldEqual DispatcherProduct(
        BRAND = Option.empty,
        PRD_INTEGRATION_ID = "country-code~source-name~source-entity-id",
        COUNTRY_CODE = "country-code",
        UNIT_PRICE_CURRENCY = Option.empty,
        EAN_CODE = Option.empty,
        EAN_CODE_DISPATCH_UNIT = Option.empty,
        DELETE_FLAG = false,
        PRODUCT_NAME = "product-name",
        CREATED_AT = "2015-06-30 13:49:00",
        UPDATED_AT = "2015-06-30 13:49:00",
        MRDR_CODE = Option.empty,
        SOURCE = "source-name",
        SUB_BRAND = Option.empty,
        SUB_CATEGORY = Option.empty,
        ITEM_TYPE = Option.empty,
        UNIT = Option.empty,
        UNIT_PRICE = FORMATTED_BIG_DECIMAL.some,
        CATEGORY = Option.empty
      )
    }
  }
}
