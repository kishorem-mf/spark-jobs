package com.unilever.ohub.spark

import SharedSparkSession._

class DomainDataProviderSpec extends SparkJobSpec {

  describe("DomainDataProvider") {
    it("should have valid country data for all countries") {
      val domainDataProvider = DomainDataProvider(spark)

      domainDataProvider.countries.size shouldBe 250
    }
  }
}
