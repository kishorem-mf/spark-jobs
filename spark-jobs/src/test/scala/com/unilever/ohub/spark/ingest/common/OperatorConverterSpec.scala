package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.ingest.{ CsvDomainConfig, CsvDomainGateKeeperSpec }

class OperatorConverterSpec extends CsvDomainGateKeeperSpec[Operator] {

  override val SUT = OperatorConverter

  describe("common operator converter") {
    it("should convert an operator correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_OPERATORS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 6

        val actualOperator = actualDataSet.head()

        val expectedOperator = Operator(
          id = "id-3",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "TR~KANGAROO~HG_226466866",
          countryCode = "TR",
          customerType = "OPERATOR",
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:47:37")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:47:37")),
          isActive = true,
          isGoldenRecord = false,
          name = Some("Kebapçim Ali - Bodrum"),
          sourceName = "KANGAROO",
          sourceEntityId = "HG_226466866",
          ohubId = actualOperator.ohubId,
          ohubCreated = actualOperator.ohubCreated,
          ohubUpdated = actualOperator.ohubUpdated,
          averagePrice = None,
          chainId = Some("UG_1"),
          chainName = Some("Diger Müsteriler"),
          channel = Some("Restaurants"),
          city = Some("Mugla"),
          cookingConvenienceLevel = None,
          countryName = Some("Turkey"),
          daysOpen = None,
          distributorName = None,
          distributorOperatorId = None,
          emailAddress = None,
          faxNumber = None,
          hasDirectMailOptIn = Some(true),
          hasDirectMailOptOut = None,
          hasEmailOptIn = Some(true),
          hasEmailOptOut = None,
          hasFaxOptIn = Some(true),
          hasFaxOptOut = None,
          hasGeneralOptOut = None,
          hasMobileOptIn = Some(true),
          hasMobileOptOut = None,
          hasTelemarketingOptIn = Some(true),
          hasTelemarketingOptOut = None,
          houseNumber = None,
          houseNumberExtension = None,
          isNotRecalculatingOtm = None,
          isOpenOnFriday = Some(true),
          isOpenOnMonday = Some(true),
          isOpenOnSaturday = Some(true),
          isOpenOnSunday = None,
          isOpenOnThursday = Some(true),
          isOpenOnTuesday = Some(true),
          isOpenOnWednesday = Some(true),
          isPrivateHousehold = None,
          kitchenType = None,
          mobileNumber = Some("05356800669"),
          netPromoterScore = Some(BigDecimal("0")),
          oldIntegrationId = None,
          otm = None,
          otmEnteredBy = None,
          phoneNumber = Some("0252 3854478"),
          region = Some("Mugla"),
          salesRepresentative = Some("Bodrum (Vacant)"),
          state = Some("Bodrum"),
          street = Some("Yalikavak Mahallesi, Çökertme Caddesi, No 187, Bodrum, Mugla"),
          subChannel = Some("Kebapçi"),
          totalDishes = Some(120),
          totalLocations = None,
          totalStaff = None,
          vat = Some("18808137040"),
          webUpdaterId = None,
          weeksClosed = None,
          zipCode = None,
          additionalFields = Map(),
          ingestionErrors = Map()
        )

        actualOperator shouldBe expectedOperator
      }
    }
  }
}
