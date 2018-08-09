package com.unilever.ohub.spark.ingest.web_event_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Subscription
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec


class SubscriptionConverterSpec extends CsvDomainGateKeeperSpec[Subscription] {
  override val SUT = SubscriptionConverter

  describe("web event subscription converter") {
    it("should convert a subscription correctly from a valid web event csv input") {
      val inputFile = "src/test/resources/WEB_EVENT_SUBSCRIPTIONS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualSubscription = actualDataSet.head()
        val expectedSubscription = Subscription(
        concatId                =     "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        countryCode             =     "DE",
        customerType            =     "SUBSCRIPTION",
        dateCreated             =     None,
        dateUpdated             =     None,
        isActive                =     true,
        isGoldenRecord          =     false,
        sourceEntityId          =     "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        sourceName              =     "EMAKINA",
        ohubId                  =     None,
        ohubCreated             =     actualSubscription.ohubCreated,
        ohubUpdated             =     actualSubscription.ohubCreated,
        contactPersonConcatId   =     "DE~b3a6208c-d7f6-44e2-80e2-f26d461f64c0~138175",
        communicationChannel    =     Some("Some channel"),
        subscriptionType        =     "Newsletter",
        hasSubscription         =     true,
        subscriptionDate        =    actualSubscription.subscriptionDate,
        hasConfirmedSubscription =   Some(true),
        ConfirmedSubscriptionDate =  Some(Timestamp.valueOf("2018-07-17 15:51:30.0")),
        additionalFields          =   Map(),
        ingestionErrors           =   Map()
        )

      actualSubscription shouldBe expectedSubscription

        // placeholder for unit test
      }
    }
  }
}
