package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.CampaignClick
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class CampaignClickConverterSpec extends CsvDomainGateKeeperSpec[CampaignClick] {

  override val SUT = CampaignClickConverter

  describe("common campaignClick converter") {
    it("should convert a campaignClick correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_CAMPAIGN_CLICKS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualCampaignClick = actualDataSet.head()

        val expectedCampaignClick = CampaignClick(
          id = "e1b9ab81-97bd-4327-8b91-fb0714a1d7fb",
          creationTimestamp = new Timestamp(1545931130186L),
          concatId = "160568290~US~MM-INIT-OPER~830499~2018-10-08T22:53:51",
          countryCode = "US",
          customerType = CampaignClick.customerType,
          sourceEntityId = "160568290~US~MM-INIT-OPER~830499",
          campaignConcatId = "160568290~US~MM-INIT-OPER~830499~155731510",
          sourceName = "ACM",
          isActive = true,
          ohubCreated = actualCampaignClick.ohubCreated,
          ohubUpdated = actualCampaignClick.ohubUpdated,
          dateCreated = None,
          dateUpdated = None,
          ohubId = Option.empty,
          isGoldenRecord = true,

          trackingId = "7140040",
          clickedUrl = "https://www.unileverfoodsolutions.us/newsletter-unsubscribe.html?id=<%=escapeUrl(encryptDES(\"8574827493847364\", recipient.partyId, \"CBC\", \"0000000000000000\"))%>",
          clickDate = Timestamp.valueOf("2016-10-03 12:33:19.0"),
          communicationChannel = "Email",
          campaignId = "155731510",
          campaignName = Some("IP_Warm_Up"),
          deliveryId = "160568290",
          deliveryName = "NAM_Warmup_US",
          contactPersonConcatId = None,
          contactPersonOhubId = "830499",
          isOnMobileDevice = false,
          operatingSystem = Some("Windows 7"),
          browserName = Some("Chrome"),
          browserVersion = Some("53.0.2785."),
          operatorConcatId = None,
          operatorOhubId = Option.empty,
          deliveryLogId = Option.empty,

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualCampaignClick shouldBe expectedCampaignClick
      }
    }
  }
}
