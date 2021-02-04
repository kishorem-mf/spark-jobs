package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperators
import com.unilever.ohub.spark.export.ddl.model.DdlOperator

class OperatorDdlConverterSpec extends SparkJobSpec with TestOperators {

  val operatorToConvert = defaultOperator.copy(
    concatId = "AU~WUFOO~AB123",
    localChannel = Some("local-channel"),
    channelUsage = Some("channel-usage"),
    socialCommercial = Some("social-commercial"),
    strategicChannel = Some("strategic-channel"),
    globalChannel = Some("global-channel"),
    globalSubChannel = Some("global-sub-channel"),
    wayOfServingAlcohol = Some("wayOfServingAlcohol")
  )
  val SUT = OperatorDdlConverter


  describe("operator ddl converter") {
    it("should convert a domain operator correctly into an ddl operator") {

      val result = SUT.convert(operatorToConvert)

      val expectedDdlOperator = DdlOperator(
        id = "id-1",
        concatId = "AU~WUFOO~AB123",
        countryCode = "country-code",
        customerType = "OPERATOR",
        dateCreated = "2017-10-16 06:09:49:000",
        dateUpdated = "2017-10-16 06:09:49:000",
        isActive = "Y",
        isGoldenRecord = "N",
        ohubId = defaultOperator.ohubId.get,
        name = "operator-name",
        sourceEntityId = "source-entity-id",
        sourceName = "source-name",
        ohubCreated = "2017-10-16 06:09:49:000",
        ohubUpdated = "2017-10-16 06:09:49:000",
        annualTurnover = "21312312312312.00",
        averagePrice = "12345.00",
        averageRating = "3",
        beveragePurchasePotential = "3444444.00",
        buildingSquareFootage = "1234m2",
        chainId = "chain-id",
        chainName = "chain-name",
        channel = "channel",
        city = "city",
        cookingConvenienceLevel = "cooking-convenience-level",
        countryName = "country-name",
        daysOpen = "4",
        distributorName = "distributor-name",
        distributorOperatorId = "",
        emailAddress = "email-address@some-server.com",
        faxNumber = "+31123456789",
        hasDirectMailOptIn = "Y",
        hasDirectMailOptOut = "N",
        hasEmailOptIn = "Y",
        hasEmailOptOut = "N",
        hasFaxOptIn = "Y",
        hasFaxOptOut = "N",
        hasGeneralOptOut = "N",
        hasMobileOptIn = "Y",
        hasMobileOptOut = "N",
        hasTelemarketingOptIn = "Y",
        hasTelemarketingOptOut = "N",
        headQuarterAddress = "Street 12",
        headQuarterCity = "Amsterdam",
        headQuarterPhoneNumber = "+31 2131231",
        headQuarterState = "North Holland",
        headQuarterZipCode = "1111AA",
        houseNumber = "12",
        houseNumberExtension = "",
        isNotRecalculatingOtm = "Y",
        isOpenOnFriday = "Y",
        isOpenOnMonday = "N",
        isOpenOnSaturday = "Y",
        isOpenOnSunday = "N",
        isOpenOnThursday = "Y",
        isOpenOnTuesday = "Y",
        isOpenOnWednesday = "Y",
        isPrivateHousehold = "N",
        kitchenType = "kitchen-type",
        menuKeywords = "meat, fish",
        mobileNumber = "+31612345678",
        netPromoterScore = "75.00",
        numberOfProductsFittingInMenu = "1",
        numberOfReviews = "3",
        oldIntegrationId = "",
        operatorLeadScore = "1",
        otm = "D",
        otmEnteredBy = "otm-entered-by",
        phoneNumber = "+31123456789",
        potentialSalesValue = "123.00",
        region = "region",
        salesRepresentative = "sales-representative",
        state = "state",
        street = "street",
        subChannel = "sub-channel",
        totalDishes = "150",
        totalLocations = "25",
        totalStaff = "105",
        vat = "vat",
        wayOfServingAlcohol = "wayOfServingAlcohol",
        website = "www.google.com",
        webUpdaterId = "web-updater-id",
        weeksClosed = "2",
        yearFounded = "1900",
        zipCode = "1234 AB",
        localChannel = "local-channel",
        channelUsage = "channel-usage",
        socialCommercial = "social-commercial",
        strategicChannel = "strategic-channel",
        globalChannel = "global-channel",
        globalSubChannel = "global-sub-channel",
        ufsClientNumber = "ufsClientNumber",
        department  = "OOH"
        )

      result shouldBe expectedDdlOperator
    }
  }

}
