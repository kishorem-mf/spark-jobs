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
          dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
          dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00.0")),
          isActive = true,
          isGoldenRecord = false,
          name = "Down under",
          sourceName = "WEB_EVENT",
          sourceEntityId = "E1-1234",
          ohubId = actualOperator.ohubId,
          ohubCreated = actualOperator.ohubCreated,
          ohubUpdated = actualOperator.ohubUpdated,
          averagePrice = Some(BigDecimal(29.95)),
          chainId = Some("bk123"),
          chainName = Some("BURGER KING"),
          channel = Some("Restaurants"),
          city = Some("Melbourne"),
          cookingConvenienceLevel = Some("MEDIUM"),
          countryName = Some("Australia"),
          daysOpen = Some(6),
          distributorName = Some("SLIGRO"),
          distributorOperatorId = Some("BV4123"),
          emailAddress = Some("info@downunder.au"),
          faxNumber = Some("61396621811"),
          hasDirectMailOptIn = Some(true),
          hasDirectMailOptOut = Some(true),
          hasEmailOptIn = Some(true),
          hasEmailOptOut = Some(true),
          hasFaxOptIn = Some(true),
          hasFaxOptOut = Some(true),
          hasGeneralOptOut = Some(true),
          hasMobileOptIn = Some(true),
          hasMobileOptOut = Some(true),
          hasTelemarketingOptIn = Some(true),
          hasTelemarketingOptOut = Some(true),
          houseNumber = Some("134"),
          houseNumberExtension = Some("A"),
          isNotRecalculatingOtm = Some(true),
          isOpenOnFriday = Some(true),
          isOpenOnMonday = Some(true),
          isOpenOnSaturday = Some(true),
          isOpenOnSunday = Some(true),
          isOpenOnThursday = Some(true),
          isOpenOnTuesday = Some(true),
          isOpenOnWednesday = Some(true),
          isPrivateHousehold = Some(true),
          kitchenType = Some("Japanese"),
          mobileNumber = Some("61612345678"),
          netPromoterScore = Some(BigDecimal(44556.0)),
          oldIntegrationId = Some("I-2345"),
          otm = Some("A"),
          otmEnteredBy = Some("Set by sales rep"),
          phoneNumber = Some("61396621811"),
          region = Some("Victoria"),
          salesRepresentative = Some("Hans Jansen"),
          state = Some("Alabama"),
          street = Some("Main street"),
          subChannel = Some("Pub"),
          totalDishes = Some(175),
          totalLocations = Some(1),
          totalStaff = Some(25),
          vat = Some("9864758522"),
          webUpdaterId = Option.empty,
          weeksClosed = Some(1),
          zipCode = Some("3006"),
          additionalFields = Map(),
          ingestionErrors = Map()
        )

        actualOperator shouldBe expectedOperator
      }
    }
    it("should select the latest operator based on dateUpdated") {
      val inputFile = "src/test/resources/WEB_EVENT_OPERATORS_DUPLICATES.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 2

        val res = actualDataSet.collect

        val emptyDateUpdated = res.filter(_.countryCode == "NZ")
        emptyDateUpdated.length shouldBe 1

        val filledDateUpdated = res.filter(_.countryCode == "AU")
        filledDateUpdated.length shouldBe 1
        filledDateUpdated.head.street shouldBe Some("Some street")
      }
    }
  }
}
