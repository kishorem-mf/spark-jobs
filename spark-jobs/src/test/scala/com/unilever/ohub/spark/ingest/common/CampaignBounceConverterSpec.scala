package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.CampaignBounce
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class CampaignBounceConverterSpec extends CsvDomainGateKeeperSpec[CampaignBounce] {

  override val SUT = CampaignBounceConverter

  describe("common campaignBounce converter") {
    it("should convert a campaignBounce correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_CAMPAIGN_BOUNCES.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualCampaignBounce = actualDataSet.head()

        val expectedCampaignBounce = CampaignBounce(
          id = "d7d0d4c8-494e-4545-98c7-9217112c67f3",
          creationTimestamp = new Timestamp(1545930619021L),
          concatId = "ZA~ACM~0~139221322",
          countryCode = "ZA",
          customerType = CampaignBounce.customerType,
          sourceEntityId = "139221322",
          sourceName = "ACM",
          isActive = true,
          ohubCreated = actualCampaignBounce.ohubCreated,
          ohubUpdated = actualCampaignBounce.ohubUpdated,
          dateCreated = None,
          dateUpdated = None,
          ohubId = Option.empty,
          isGoldenRecord = true,

          deliveryLogId = "26763007",
          campaignId = "0",
          campaignName = None,
          deliveryId = "139221322",
          deliveryName = "ZA Welcome - Complete Profile",
          communicationChannel = "Mobile (SMS)",
          contactPersonConcatId = "ZA~EMAKINA~42E96AEDDA9B18AFD4D2499F0086BB61E4712F07",
          contactPersonOhubId = Option.empty,
          bounceDate = Timestamp.valueOf("2016-09-24 16:22:45.0"),
          failureType = "Invalid domain",
          failureReason = "Invalid domain",
          isControlGroupMember = false,
          isProofGroupMember = false,
          operatorConcatId = Some("ZA~EMAKINA~42E96AEDDA9B18AFD4D2499F0086BB61E4712F07"),
          operatorOhubId = Option.empty,

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualCampaignBounce shouldBe expectedCampaignBounce
      }
    }
  }
}
