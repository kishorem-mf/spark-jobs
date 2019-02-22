package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.CampaignOpen
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class CampaignOpenConverterSpec extends CsvDomainGateKeeperSpec[CampaignOpen] {

  override val SUT = CampaignOpenConverter

  describe("common campaignOpen converter") {
    it("should convert a campaignOpen correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_CAMPAIGN_OPENS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualCampaignOpen = actualDataSet.head()

        val expectedCampaignOpen = CampaignOpen(
          id = "903610ee-1f5d-44cd-ad11-994f4fd0f677",
          creationTimestamp = new Timestamp(1545930619515L),
          concatId = "b3a6208c~NL~EMAKINA~1003499146~2018-10-08T22:53:51",
          countryCode = "ZA",
          customerType = CampaignOpen.customerType,
          sourceEntityId = "b3a6208c~NL~EMAKINA~1003499146",
          campaignConcatId = "b3a6208c~NL~EMAKINA~1003499146~f26d461f64c0",
          sourceName = "ACM",
          isActive = true,
          ohubCreated = actualCampaignOpen.ohubCreated,
          ohubUpdated = actualCampaignOpen.ohubUpdated,
          dateCreated = None,
          dateUpdated = None,
          ohubId = Option.empty,
          isGoldenRecord = true,

          trackingId = "5458011",
          campaignId = "012",
          campaignName = Some("adfghh"),
          deliveryId = "139221322",
          deliveryName = "ZA Welcome - Complete Profile",
          communicationChannel = "Mobile (SMS)",
          contactPersonConcatId = "ZA~EMAKINA~4e438846-f6c2-4239-b59e-33b69b6a5587",
          contactPersonOhubId = Option.empty,
          operatorConcatId = Some("ZA~EMAKINA~4e438846-f6c2-4239-b59e-33b69b6a5587"),
          operatorOhubId = Option.empty,
          openDate = Timestamp.valueOf("2016-07-24 09:03:36.0"),
          deliveryLogId = Some("1wewee2"),

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualCampaignOpen shouldBe expectedCampaignOpen
      }
    }
  }
}
