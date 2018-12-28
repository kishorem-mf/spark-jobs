package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.ingest.{ CsvDomainConfig, CsvDomainGateKeeperSpec }

class DomainGateKeeperDedupeSpec extends CsvDomainGateKeeperSpec[Operator] {

  override val SUT = OperatorConverter

  describe("domain gatekeeper deduplication") {

    it("should select the latest entity based on several date columns") {
      val inputFile = "src/test/resources/COMMON_OPERATORS_DUPLICATES.csv"
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = ";")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 3

        val res = actualDataSet.collect

        // by default pick newest by dateUpdated
        val filledDateUpdated = res.filter(_.countryCode == "AU")
        filledDateUpdated.length shouldBe 1
        filledDateUpdated.head.street shouldBe Some("Some street")

        // when missing dateUpdated fall back to dateCreated
        val filledDateCreated = res.filter(_.countryCode == "FR")
        filledDateCreated.length shouldBe 1
        filledDateCreated.head.street shouldBe Some("Main street")

        // as a tie-breaker use ohubUpdated
        val fallback = res.filter(_.countryCode == "DE")
        fallback.length shouldBe 1
        fallback.head.street shouldBe Some("Some street")

      }
    }
  }
  
}
