package com.unilever.ohub.spark.domain.entity

import org.scalatest.{ Matchers, WordSpec }

class OperatorSpec extends WordSpec with Matchers with TestOperators {

  "Operator" should {
    "be created correctly (no exception is thrown)" when {
      "only valid data is provided" in {
        defaultOperator.name shouldBe "operator-name"
      }
    }
  }
}
