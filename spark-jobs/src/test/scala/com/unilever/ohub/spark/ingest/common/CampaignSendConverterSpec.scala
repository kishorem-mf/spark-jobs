package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.CampaignSend
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class CampaignSendConverterSpec extends CsvDomainGateKeeperSpec[CampaignSend] {

  override val SUT = CampaignSendConverter

  describe("common campaignSend converter") {
    it("should convert a campaignSend correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_CAMPAIGN_SENDS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualCampaignSend = actualDataSet.head()

        val expectedCampaignSend = CampaignSend(
          id = "75ec0f22-a0f0-48a2-8771-89e9e55bdd7d",
          creationTimestamp = new Timestamp(1545927397428L),
          concatId = "b3a6208c~NL~EMAKINA~1003499146~2018-10-08T22:55:15",
          countryCode = "RU",
          customerType = CampaignSend.customerType,
          sourceEntityId = "b3a6208c~NL~EMAKINA~1003499146",
          campaignConcatId = "b3a6208c~NL~EMAKINA~1003499146~f26d461f64c0",
          sourceName = "ACM",
          isActive = true,
          ohubCreated = actualCampaignSend.ohubCreated,
          ohubUpdated = actualCampaignSend.ohubUpdated,
          dateCreated = None,
          dateUpdated = None,
          ohubId = Option.empty,
          isGoldenRecord = true,

          deliveryLogId = "121600488",
          campaignId = "f26d461f64c0",
          campaignName = Some("20181003_Hellmann's_promo_RU_ru_ru"),
          deliveryId = "b3a6208c",
          deliveryName = "20181003_Hellmann's_promo_RU_ru_ru_A_final",
          communicationChannel = "Email",
          operatorConcatId = Some("RU~EMAKINA~B5601B8E737507C12F08BA60E486E876954272B7"),
          operatorOhubId = Option.empty,
          sendDate = Timestamp.valueOf("2018-10-08 22:55:15.0"),
          isControlGroupMember = false,
          isProofGroupMember = false,
          selectionForOfflineChannels = "70652",
          contactPersonConcatId = "RU~EMAKINA~B5601B8E737507C12F08BA60E486E876954272B7",
          contactPersonOhubId = Option.empty,
          waveName = "20181003_Hellmann's_promo_RU_ru_ru~20181003_Hellmann's_promo_RU_ru_ru_A_final",

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualCampaignSend shouldBe expectedCampaignSend
      }
    }
  }
}
