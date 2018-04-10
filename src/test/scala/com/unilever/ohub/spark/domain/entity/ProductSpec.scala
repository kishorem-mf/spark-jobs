package com.unilever.ohub.spark.domain.entity

import org.scalatest.{ Matchers, WordSpec }

class ProductSpec extends WordSpec with Matchers with TestProducts {

  "Product" should {
    "be created correctly (no exception is thrown)" when {
      "only valid data is provided" in {
        defaultProductRecord.name shouldBe "product-name"
      }
    }
  }
}
