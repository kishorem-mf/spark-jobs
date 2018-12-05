package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class SubscriptionConverterSpec extends CsvDomainGateKeeperSpec[Subscription] {

  override val SUT = SubscriptionConverter

  describe("common subscription converter") {
    it("should convert a subscription correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_SUBSCRIPTION.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualSubscription = actualDataSet.head()

        val expectedSubscription = Subscription(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "AU~EMAKINA~S1234",
          contactPersonOhubId = Option.empty,
          countryCode = "AU",
          customerType = "SUBSCRIPTION",
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00.0")),
          isActive = true,
          isGoldenRecord = false,
          sourceEntityId = "S1234",
          sourceName = "EMAKINA",
          ohubId = None,
          ohubCreated = actualSubscription.ohubCreated,
          ohubUpdated = actualSubscription.ohubCreated,
          contactPersonConcatId = "AU~EMAKINA~AB123",
          communicationChannel = Some("channel"),
          subscriptionType = "default_newsletter_opt_in",
          hasSubscription = true,
          subscriptionDate = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
          hasConfirmedSubscription = Some(true),
          confirmedSubscriptionDate = Some(Timestamp.valueOf("2015-06-30 13:48:00.0")),
          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualSubscription shouldBe expectedSubscription
      }
    }
  }
}
