package com.unilever.ohub.spark.ingest.web_event_interface

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec
import cats.syntax.option._

class OperatorConverterSpec extends CsvDomainGateKeeperSpec[Operator] {

  override val SUT = OperatorConverter

  describe("web event operator converter") {
    it("should convert an operator correctly from a valid web event csv input") {
      val inputFile = "src/test/resources/WEB_EVENT_OPERATORS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOperator = actualDataSet.head()
        val expectedOperator = Operator(
          concatId = "AU~EMAKINA~E1-1234",
          countryCode = "AU",
          customerType = "OPERATOR",
          dateCreated = none,
          dateUpdated = none,
          isActive = true,
          isGoldenRecord = false,
          name = "Down under",
          sourceName = "EMAKINA",
          sourceEntityId = "E1-1234",
          ohubId = actualOperator.ohubId,
          ohubCreated = actualOperator.ohubCreated,
          ohubUpdated = actualOperator.ohubUpdated,
          averagePrice = none,
          chainId = none,
          chainName = none,
          channel = "Restaurants",
          city = "Melbourne",
          cookingConvenienceLevel = none,
          countryName = "Australia",
          daysOpen = none,
          distributorName = "SLIGRO",
          distributorOperatorId = "BV4123",
          emailAddress = none,
          faxNumber = none,
          hasDirectMailOptIn = none,
          hasDirectMailOptOut = none,
          hasEmailOptIn = none,
          hasEmailOptOut = none,
          hasFaxOptIn = none,
          hasFaxOptOut = none,
          hasGeneralOptOut = none,
          hasMobileOptIn = none,
          hasMobileOptOut = none,
          hasTelemarketingOptIn = none,
          hasTelemarketingOptOut = none,
          houseNumber = "134",
          houseNumberExtension = "A",
          isNotRecalculatingOtm = none,
          isOpenOnFriday = true,
          isOpenOnMonday = true,
          isOpenOnSaturday = true,
          isOpenOnSunday = true,
          isOpenOnThursday = true,
          isOpenOnTuesday = true,
          isOpenOnWednesday = true,
          isPrivateHousehold = true,
          kitchenType = "Japanese",
          mobileNumber = none,
          netPromoterScore = none,
          oldIntegrationId = none,
          otm = none,
          otmEnteredBy = none,
          phoneNumber = none,
          region = none,
          salesRepresentative = none,
          state = "Alabama",
          street = "Main street",
          subChannel = none,
          totalDishes = 175,
          totalLocations = 1,
          totalStaff = 25,
          vat = "9864758522",
          webUpdaterId = none,
          weeksClosed = none,
          zipCode = "3006",
          additionalFields = Map(),
          ingestionErrors = Map()
        )

        actualOperator shouldBe expectedOperator
      }
    }
  }
}
