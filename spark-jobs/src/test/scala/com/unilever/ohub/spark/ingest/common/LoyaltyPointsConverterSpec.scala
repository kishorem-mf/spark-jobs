package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.LoyaltyPoints
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class LoyaltyPointsConverterSpec extends CsvDomainGateKeeperSpec[LoyaltyPoints] {

  override val SUT = LoyaltyPointsConverter;

  describe("common loyaltyPoints converter") {
    it("should convert a loyaltyPoints correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_LOYALTY_POINTS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualLoyaltyPoints = actualDataSet.head()

        val expectedLoyaltyPoints = LoyaltyPoints(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "DE~EMAKINA~123",
          countryCode = "DE",
          customerType = "CONTACTPERSON",
          sourceEntityId = "123",
          sourceName = "EMAKINA",
          isActive = true,
          ohubCreated = actualLoyaltyPoints.ohubCreated,
          ohubUpdated = actualLoyaltyPoints.ohubUpdated,
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          ohubId = Option.empty,
          isGoldenRecord = true,

          totalEarned = Some(BigDecimal.apply(15.0)),
          totalSpent = Some(BigDecimal.apply(12.0)),
          totalActual = Some(BigDecimal.apply(3.0)),
          rewardGoal = Some(BigDecimal.apply(20.0)),
          contactPersonRefId = None,
          contactPersonConcatId = Some("DE~EMAKINA~456"),
          contactPersonOhubId = None,
          operatorConcatId = Some("DE~EMAKINA~789"),
          operatorOhubId = None,

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualLoyaltyPoints shouldBe expectedLoyaltyPoints
      }
    }
  }
}
