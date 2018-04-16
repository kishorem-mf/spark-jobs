package com.unilever.ohub.spark.tsv2parquet.fuzzit

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.tsv2parquet.DomainGateKeeperSpec

class OperatorConverterSpec extends DomainGateKeeperSpec[Operator] {

  private[tsv2parquet] override val SUT = OperatorConverter

  describe("fuzzit operator converter") {
    it("should convert an operator correctly from a valid fuzzit csv input") {
      val inputFile = "src/test/resources/FUZZIT_OPERATORS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOperator = actualDataSet.head()
        val expectedOperator = Operator(
          concatId = "DE~FUZZIT~02700856-30A6-44CE-BC9A-4FC3D6935088",
          countryCode = "DE",
          customerType = "operator",
          dateCreated = Some(Timestamp.valueOf("2018-01-12 13:20:58.7")),
          dateUpdated = Some(Timestamp.valueOf("2018-01-12 13:20:58.7")),
          isActive = true,
          isGoldenRecord = false,
          ohubId = None,
          name = "Institut f�r Virologie und Immunologie IVI",
          sourceEntityId = "02700856-30A6-44CE-BC9A-4FC3D6935088",
          sourceName = "FUZZIT",
          ohubCreated = actualOperator.ohubCreated,
          ohubUpdated = actualOperator.ohubUpdated,
          averagePrice = None,
          chainId = None,
          chainName = None,
          channel = None,
          city = Some("Mittelh�usern"),
          cookingConvenienceLevel = None,
          countryName = Some("Germany"),
          daysOpen = None,
          distributorName = None,
          distributorOperatorId = None,
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
          houseNumber = Some("293"),
          houseNumberExtension = None,
          isNotRecalculatingOtm = None,
          isOpenOnFriday = None,
          isOpenOnMonday = None,
          isOpenOnSaturday = None,
          isOpenOnSunday = None,
          isOpenOnThursday = None,
          isOpenOnTuesday = None,
          isOpenOnWednesday = None,
          isPrivateHousehold = None,
          kitchenType = None,
          mobileNumber = None,
          netPromoterScore = None,
          oldIntegrationId = None,
          otm = None,
          otmEnteredBy = None,
          phoneNumber = None,
          region = None,
          salesRepresentative = None,
          state = None,
          street = Some("Sensemattstrasse"),
          subChannel = None,
          totalDishes = None,
          totalLocations = None,
          totalStaff = None,
          vat = None,
          webUpdaterId = None,
          weeksClosed = None,
          zipCode = Some("3147"),
          additionalFields = Map("germanChainId" -> "german-chain-id", "germanChainName" -> "german-chain-name"),
          ingestionErrors = Map()
        )

        actualOperator shouldBe expectedOperator
      }
    }
  }
}
