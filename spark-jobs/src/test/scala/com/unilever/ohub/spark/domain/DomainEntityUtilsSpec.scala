package com.unilever.ohub.spark.domain

import com.unilever.ohub.spark.SimpleSpec

class DomainEntityUtilsSpec extends SimpleSpec {
  val SUT = DomainEntityUtils

  describe("DomainEntityUtils") {
    it("Should be able to get the DomainEntityCompanion object for all DomainEntities") {
      try {
        SUT.domainCompanionObjects
        succeed
      } catch {
        case e: ClassCastException => fail("Did you add a companionObject of type DomainEntityCompanion to new DomainEntities?", e)
        case e: Throwable ⇒ fail("Unexpected exception thrown", e)
      }
    }
  }
}
