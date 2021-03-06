package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Campaign
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class CampaignConverterSpec extends CsvDomainGateKeeperSpec[Campaign] {

  override val SUT = CampaignConverter

  describe("common campaign converter") {
    it("should convert a campaign correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_CAMPAIGNS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualCampaign = actualDataSet.head()

        val expectedCampaign = Campaign(
          id = "bd433fc0-b6f4-4dfd-a53d-a499ba636d0a",
          creationTimestamp = new Timestamp(1545929029010L),
          concatId = "b3a6208c~NL~EMAKINA~1003499146~2018-10-08T22:53:51",
          countryCode = "US",
          customerType = Campaign.customerType,
          sourceEntityId = "347696686",
          campaignConcatId = "b3a6208c~NL~EMAKINA~1003499146",
          sourceName = "ACM",
          isActive = true,
          ohubCreated = actualCampaign.ohubCreated,
          ohubUpdated = actualCampaign.ohubUpdated,
          dateCreated = None,
          dateUpdated = None,
          ohubId = Option.empty,
          isGoldenRecord = true,

          contactPersonConcatId = None,
          contactPersonOhubId = "999654",
          campaignId = "345882796",
          campaignName = Some("20180320_IC_AO_MAR19AfricaRisingNewsletter_NAM_us_en"),
          deliveryId = "347696686",
          deliveryName = "20180320_IC_AO_MAR19AfricaRisingNewsletter_NAM_us_en_A final",
          campaignSpecification = Some("(None specified)"),
          campaignWaveStartDate = Timestamp.valueOf("2018-03-16 00:00:00.0"),
          campaignWaveEndDate = Timestamp.valueOf("2018-03-27 00:00:00.0"),
          campaignWaveStatus = "Sent",

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualCampaign shouldBe expectedCampaign
      }
    }
  }
}
