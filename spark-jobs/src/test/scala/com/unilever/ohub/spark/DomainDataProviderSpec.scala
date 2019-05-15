package com.unilever.ohub.spark

class DomainDataProviderSpec extends SparkJobSpec {
  describe("DomainDataProvider") {
    val domainDataProvider = DomainDataProvider()

    it("should be able to create a dataset for channelReferences") {
      val channelReferences = domainDataProvider.channelReferences
      channelReferences.size > 0 shouldBe true
    }

    it("should be able to create a map with sourcePreferences") {
      val sourcePreferences = domainDataProvider.sourcePreferences
      sourcePreferences.size > 0 shouldBe true
    }
  }
}
