package com.unilever.ohub.spark.ingest.ohub1

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class SubscriptionConverterSpec extends CsvDomainGateKeeperSpec[Subscription] {

  override val SUT = SubscriptionConverter

  describe("ohub1 subscription converter") {
    it("should convert an subscription correctly from a valid ohub1 csv input") {
      val inputFile = "src/test/resources/OHUB1_SUBSCRIPTION.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualSubscription = actualDataSet.head()

        val expectedSubscription = Subscription(
          concatId = "AU~EMAKINA~AU~DEFAULT_NEWSLETTER_OPT_IN",
          countryCode = "AU",
          customerType = "SUBSCRIPTION",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00.0")),
          isActive = true,
          isGoldenRecord = false,
          sourceEntityId = "AU~DEFAULT_NEWSLETTER_OPT_IN",
          sourceName = "EMAKINA",
          ohubId = None,
          ohubCreated = actualSubscription.ohubCreated,
          ohubUpdated = actualSubscription.ohubCreated,
          contactPersonConcatId = "AU~EMAKINA~AB123",
          communicationChannel = Some("Some channel"),
          subscriptionType = "default_newsletter_opt_in",
          hasSubscription = true,
          subscriptionDate = Timestamp.valueOf("2015-06-30 13:47:00.0"),
          hasConfirmedSubscription = Some(true),
          ConfirmedSubscriptionDate = Some(Timestamp.valueOf("2015-06-30 13:48:00.0")),
          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualSubscription shouldBe expectedSubscription
      }
    }
  }
}
