package com.unilever.ohub.spark.domain

import com.unilever.ohub.spark.SimpleSpec

class DomainEntityUtilsSpec extends SimpleSpec {
  val SUT = DomainEntityUtils

  describe("DomainEntityUtils") {
    it("Should be able to get the DomainEntityCompanion object for all DomainEntities") {
      SUT.domainCompanionObjects.length > 0 shouldBe true
    }
  }
}
