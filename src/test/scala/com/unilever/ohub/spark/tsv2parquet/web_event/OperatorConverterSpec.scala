package com.unilever.ohub.spark.tsv2parquet.web_event

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.tsv2parquet.CsvDomainGateKeeperSpec

class OperatorConverterSpec extends CsvDomainGateKeeperSpec[Operator] {

  private[tsv2parquet] override val SUT = OperatorConverter

  describe("web event operator converter") {
    it("should convert an operator correctly from a valid file interface csv input") {
      val inputFile = "src/test/resources/WEB_EVENT_OPERATORS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualOperator = actualDataSet.head()
        val expectedOperator = Operator(
          concatId = "AU~WEB_EVENT~E1-1234",
          countryCode = "AU",
          customerType = "OPERATOR",
          dateCreated = Option.empty,
          dateUpdated = Option.empty,
          isActive = true,
          isGoldenRecord = false,
          name = "Down under",
          sourceName = "WEB_EVENT",
          sourceEntityId = "E1-1234",
          ohubId = actualOperator.ohubId,
          ohubCreated = actualOperator.ohubCreated,
          ohubUpdated = actualOperator.ohubUpdated,
          averagePrice = Option.empty,
          chainId = Option.empty,
          chainName = Option.empty,
          channel = Some("Restaurants"),
          city = Some("Melbourne"),
          cookingConvenienceLevel = Option.empty,
          countryName = Some("Australia"),
          daysOpen = Option.empty,
          distributorName = Some("SLIGRO"),
          distributorOperatorId = Some("BV4123"),
          emailAddress = Option.empty,
          faxNumber = Option.empty,
          hasDirectMailOptIn = Option.empty,
          hasDirectMailOptOut = Option.empty,
          hasEmailOptIn = Option.empty,
          hasEmailOptOut = Option.empty,
          hasFaxOptIn = Option.empty,
          hasFaxOptOut = Option.empty,
          hasGeneralOptOut = Option.empty,
          hasMobileOptIn = Option.empty,
          hasMobileOptOut = Option.empty,
          hasTelemarketingOptIn = Option.empty,
          hasTelemarketingOptOut = Option.empty,
          houseNumber = Some("134"),
          houseNumberExtension = Some("A"),
          isNotRecalculatingOtm = Option.empty,
          isOpenOnFriday = Some(true),
          isOpenOnMonday = Some(true),
          isOpenOnSaturday = Some(true),
          isOpenOnSunday = Some(true),
          isOpenOnThursday = Some(true),
          isOpenOnTuesday = Some(true),
          isOpenOnWednesday = Some(true),
          isPrivateHousehold = Some(true),
          kitchenType = Some("Japanese"),
          mobileNumber = Option.empty,
          netPromoterScore = Option.empty,
          oldIntegrationId = Option.empty,
          otm = Option.empty,
          otmEnteredBy = Option.empty,
          phoneNumber = Option.empty,
          region = Option.empty,
          salesRepresentative = Option.empty,
          state = Some("Alabama"),
          street = Some("Main street"),
          subChannel = Option.empty,
          totalDishes = Some(175),
          totalLocations = Some(1),
          totalStaff = Some(25),
          vat = Some("9864758522"),
          webUpdaterId = Option.empty,
          weeksClosed = Option.empty,
          zipCode = Some("3006"),
          additionalFields = Map(),
          ingestionErrors = Map()
        )

        actualOperator shouldBe expectedOperator
      }
    }

    // it("should select the latest operator based on dateUpdated") {
    //   val inputFile = "src/test/resources/WEB_EVENT_OPERATORS_DUPLICATES.csv"

    //   runJobWith(inputFile) { actualDataSet ⇒
    //     actualDataSet.count() shouldBe 2

    //     val res = actualDataSet.collect

    //     val emptyDateUpdated = res.filter(_.countryCode == "NZ")
    //     emptyDateUpdated.length shouldBe 1

    //     val filledDateUpdated = res.filter(_.countryCode == "AU")
    //     filledDateUpdated.length shouldBe 1
    //     filledDateUpdated.head.street shouldBe Some("Some street")
    //   }
    // }

  }
}
