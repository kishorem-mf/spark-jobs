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
        accountStatus = "Y",
        isGoldenRecord = "N",
        afhCustomerGoldenId = defaultOperator.ohubId.get,
        customerName = "operator-name",
        customerSAPConcatId = "source-entity-id",
        sourceName = "source-name",
        ohubCreated = "2017-10-16 06:09:49:000",
        ohubUpdated = "2017-10-16 06:09:49:000",
        annualTurnover = "21312312312312.00",
        averagePricePerMeal = "12345.00",
        averageRating = "3",
        beveragePurchasePotential = "3444444.00",
        buildingSquareFootage = "1234m2",
        chainId = "chain-id",
        chainName = "chain-name",
        segment = "channel",
        city = "city",
        convenienceLevel = "cooking-convenience-level",
        country = "country-name",
        weekOrYearOpen = "4",
        distributorName = "distributor-name",
        distributorOperatorId = "",
        email = "email-address@some-server.com",
        fax = "+31123456789",
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
        dayWeekOpen = "N",
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
        otmReason = "otm-entered-by",
        phoneNumber1 = "+31123456789",
        potentialSalesValue = "123.00",
        region = "region",
        salesRepresentative = "sales-representative",
        state = "state",
        addressLine1 = "street",
        subSegments = "sub-channel",
        numberOfMealServedPerDay = "150",
        totalLocations = "25",
        numberOfEmployees = "105",
        vat = "vat",
        wayOfServingAlcohol = "wayOfServingAlcohol",
        website = "www.google.com",
        webUpdaterId = "web-updater-id",
        weeksClosed = "2",
        yearFounded = "1900",
        postalCode = "1234 AB",
        localChannel = "local-channel",
        channelUsage = "channel-usage",
        socialCommercial = "social-commercial",
        strategicChannel = "strategic-channel",
        parentSegment = "global-channel",
        globalSubChannel = "global-sub-channel",
        crmAccountId = "",
        channel = ""
      )

      result shouldBe expectedDdlOperator
    }
  }

}
