package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestOperatorsGolden
import com.unilever.ohub.spark.export.ddl.model.DdlOperator

class OperatorDdlConverterSpec extends SparkJobSpec with TestOperatorsGolden {


  val SUT = OperatorDdlConverter
  val operatorToConvert = defaultOperatorGolden.copy(ohubId = Some("12345"), globalChannel = Some("globalChannel"))

  describe("operator ddl converter") {
    it("should convert a domain operator correctly into an ddl operator") {

      val result = SUT.convert(operatorToConvert)

      val expectedDdlOperator = DdlOperator(
        afhCustomerGoldenID = "12345",
        crmAccountID = "",
        customerSAPconcatID = "source-entity-id",
        channel = "",
        division = "",
        salesOrgID = "",
        parentSourceCustomerCode = "",
        closingTimeWorkingDay = "",
        openingTimeWorkingDay = "",
        preferredVisitDays = "",
        preferredVisitStartTime = "",
        preferredVisitEndTime = "",
        preferredDeliveryDay = "",
        preferredVisitweekofMonth = "",
        customerName = "operator-name",
        customerName2 = "",
        addressLine1 = "street",
        city = "city",
        state = "state",
        postalCode = "1234 AB",
        country = "country-name",
        region = "region",
        phoneNumber1 = "+31123456789",
        fax = "+31123456789",
        email = "email-address@some-server.com",
        otm = "D",
        otmReason = "otm-entered-by",
        segment = "channel",
        subSegments = "sub-channel",
        parentSegment = "globalChannel",
        accountType = "",
        accountStatus = "",
        webshopRegistered = "",
        loyaltyManagementOptIn = "",
        numberOfMealServedPerDay = "150",
        avgPricePerMeal = "12345.00",
        weekOrYearOpen = "4",
        foodSpendMonth = "",
        convenienceLevel = "cooking-convenience-level",
        latitude = "",
        longitude = "",
        customerHierarchyLevel3Desc = "",
        customerHierarchyLevel4Desc = "",
        customerHierarchyLevel5Desc = "",
        customerHierarchyLevel7Desc = "",
        mixedorUFS = "",
        salesGroupKey = "",
        salesOfficeKey = "",
        eccIndustryKey = "",
        salesDistrict = "",
        customerGroup = "",
        languageKey = "",
        sapCustomerID = "",
        recordType = "",
        indirectAccount = "",
        sourceName = "source-name",
        website = "www.google.com",
        keyNumber = "",
        hasWebshopAccount = "",
        dayWeekOpen = "",
        doYouHaveaTerraceOrOutsideSeating = "",
        doYouOfferTakeAways = "",
        DoYouOfferaHomeDeliveryService = "",
        howManySeatsDoYouHave = "",
        kitchenType = "",
        numberofBedsSite = "",
        numberofRoomsSite = "",
        numberofStudentsSite = "",
        numberofEmployeesGroup = "",
        foodServedOnsite = "",
        conferenceOutletOnSite = "",
        twitterURL = "",
        facebookURL = "",
        instagramURL = "",
        numberofchildsites = "",
        lastlogintowebsite = "",
        lastnewsletteropened = "",
        subcribedtoUFSnewsletter = "",
        subcribedtoIceCreamnewsletter = "",
        last3onlineevents = "",
        facebookcampaignsclicked = "",
        liveChatactivities = "",
        loyaltyRewardsBalancePoints = "",
        loyaltyRewardsBalanceupdatedDate = "",
        accountSubType = "",
        visitorsPerYear = "",
        unileverNowClassification = "",
        tradingStatus = "",
        ctps = "",
        caterlystOpportunity = "",
        repAssessedOpportunity = ""
      )
      result shouldBe expectedDdlOperator
    }
  }

}
