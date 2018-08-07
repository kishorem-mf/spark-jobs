package com.unilever.ohub.spark.ingest.emakina

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class OperatorConverterSpec extends CsvDomainGateKeeperSpec[Operator] {

  override val SUT = OperatorConverter

  describe("emakina operator converter") {
    it("should convert an operator correctly from a valid emakina csv input") {
      val inputFile = "src/test/resources/EMAKINA_CONTACT_PERSONS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOperator = actualDataSet.head()

        val expectedOperator = Operator(
          concatId = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
          countryCode = "DE",
          customerType = "OPERATOR",
          dateCreated = None,
          dateUpdated = None,
          isActive = true,
          isGoldenRecord = false,
          name = "Jägerhof",
          sourceName = "EMAKINA",
          sourceEntityId = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
          ohubId = actualOperator.ohubId,
          ohubCreated = actualOperator.ohubCreated,
          ohubUpdated = actualOperator.ohubUpdated,
          averagePrice = None,
          chainId = None,
          chainName = None,
          channel = Some("unknown"),
          city = None,
          cookingConvenienceLevel = None,
          countryName = Some("Germany"),
          daysOpen = None,
          distributorName = Some("my primary distributer"),
          distributorOperatorId = Some("my customer id"),
          emailAddress = None,
          faxNumber = None,
          hasDirectMailOptIn = None,
          hasDirectMailOptOut = None,
          hasEmailOptIn = None,
          hasEmailOptOut = None,
          hasFaxOptIn = None,
          hasFaxOptOut = None,
          hasGeneralOptOut = None,
          hasMobileOptIn = None,
          hasMobileOptOut = None,
          hasTelemarketingOptIn = None,
          hasTelemarketingOptOut = None,
          houseNumber = None,
          houseNumberExtension = None,
          isNotRecalculatingOtm = None,
          isOpenOnFriday = Some(true),
          isOpenOnMonday = Some(true),
          isOpenOnSaturday = Some(true),
          isOpenOnSunday = Some(true),
          isOpenOnThursday = Some(true),
          isOpenOnTuesday = Some(true),
          isOpenOnWednesday = Some(true),
          isPrivateHousehold = Some(true),
          kitchenType = Some("my cuisine type"),
          mobileNumber = None,
          netPromoterScore = None,
          oldIntegrationId = None,
          otm = None,
          otmEnteredBy = None,
          phoneNumber = None,
          region = None,
          salesRepresentative = None,
          state = None,
          street = None,
          subChannel = None,
          totalDishes = Some(100),
          totalLocations = Some(5),
          totalStaff = Some(10),
          vat = Some("my vat"),
          webUpdaterId = Some("my webupdater id"),
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
