package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.{Subscription, _}
import com.unilever.ohub.spark.ingest.{CsvDomainConfig, CsvDomainGateKeeperSpec}

class DomainGateKeeperSubscriptionDedupeSpec extends CsvDomainGateKeeperSpec[Subscription] with TestSubscription {

  override val SUT = SubscriptionConverter

  describe("domain gatekeeper deduplication") {

    it("should select the latest entity based on several date columns") {
      val inputFile = "src/test/resources/COMMON_SUBSCRIPTIONS_DUPLICATES.csv"
      val config = CsvDomainConfig(inputFile = inputFile, outputFile = "", fieldSeparator = ";")

      runJobWith(config) { actualDataSet ⇒
        actualDataSet.count() shouldBe 5

        val res = actualDataSet.collect

        // by default pick newest by subscriptionDate
        val filledSubscriptionDate = res.filter(_.countryCode == "AU")
        filledSubscriptionDate.length shouldBe 1
        filledSubscriptionDate.head.hasSubscription shouldBe false

        // then fall back to confirmedSubscriptionDate
        val filledConfirmedSubscriptionDate = res.filter(_.countryCode == "NZ")
        filledConfirmedSubscriptionDate.length shouldBe 1
        filledConfirmedSubscriptionDate.head.hasSubscription shouldBe false

        // then fall back to dateUpdated
        val filledDateUpdated = res.filter(_.countryCode == "NL")
        filledDateUpdated.length shouldBe 1
        filledDateUpdated.head.hasSubscription shouldBe false

        // then fall back to dateCreated
        val filledDateCreated = res.filter(_.countryCode == "FR")
        filledDateCreated.length shouldBe 1
        filledDateCreated.head.hasSubscription shouldBe false

        // as a tie-breaker use ohubUpdated
        val fallback = res.filter(_.countryCode == "DE")
        fallback.length shouldBe 1
        // fallback.head.hasSubscription shouldBe false

      }

    }
  }

}

