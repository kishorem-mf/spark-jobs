package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.ddl.model.DdlOperator
import com.unilever.ohub.spark.ingest.CustomParsers._

object OperatorDdlConverter extends Converter[Operator, DdlOperator] with TypeConversionFunctions {

  // scalastyle:off method.length
  override def convert(implicit op: Operator, explain: Boolean = false): DdlOperator = {
    DdlOperator(
      afhCustomerGoldenID = getValue("ohubId"),
      crmAccountID = Option.empty,
      customerSAPconcatID = getValue("sourceEntityId"),
      channel = Option.empty,
      division = Option.empty,
      salesOrgID = Option.empty,
      parentSourceCustomerCode = Option.empty,
      closingTimeWorkingDay = Option.empty,
      openingTimeWorkingDay = Option.empty,
      preferredVisitDays = Option.empty,
      preferredVisitStartTime = Option.empty,
      preferredVisitEndTime = Option.empty,
      preferredDeliveryDay = Option.empty,
      preferredVisitweekofMonth = Option.empty,
      customerName = getValue("name"),
      customerName2 = Option.empty,
      addressLine1 = getValue("street"),
      city = getValue("city"),
      state = getValue("state"),
      postalCode = getValue("zipCode"),
      country = getValue("countryName"),
      region = getValue("region"),
      phoneNumber1 = getValue("phoneNumber"),
      fax = getValue("faxNumber"),
      email = getValue("emailAddress"),
      otm = getValue("otm"),
      otmReason = getValue("otmEnteredBy"),
      segment = getValue("channel"),
      subSegments = getValue("subChannel"),
      parentSegment = getValue("globalChannel"),
      accountType = Option.empty,
      accountStatus = Option.empty,
      webshopRegistered = Option.empty,
      loyaltyManagementOptIn = Option.empty,
      numberOfMealServedPerDay = getValue("totalDishes"),
      avgPricePerMeal = getValue("averagePrice"),
      weekOrYearOpen = getValue("daysOpen"),
      foodSpendMonth = Option.empty,
      convenienceLevel = getValue("cookingConvenienceLevel"),
      latitude = Option.empty,
      longitude = Option.empty,
      customerHierarchyLevel3Desc = Option.empty,
      customerHierarchyLevel4Desc = Option.empty,
      customerHierarchyLevel5Desc = Option.empty,
      customerHierarchyLevel7Desc = Option.empty,
      mixedorUFS = Option.empty,
      salesGroupKey = Option.empty,
      salesOfficeKey = Option.empty,
      eccIndustryKey = Option.empty,
      salesDistrict = Option.empty,
      customerGroup = Option.empty,
      languageKey = Option.empty,
      sapCustomerID = Option.empty,
      recordType = Option.empty,
      indirectAccount = Option.empty,
      sourceName = getValue("sourceName"),
      website = getValue("website"),
      keyNumber = Option.empty,
      hasWebshopAccount = Option.empty,
      dayWeekOpen = Option.empty,
      doYouHaveaTerraceOrOutsideSeating = Option.empty,
      doYouOfferTakeAways = Option.empty,
      DoYouOfferaHomeDeliveryService = Option.empty,
      howManySeatsDoYouHave = Option.empty,
      kitchenType = Option.empty,
      numberofBedsSite = Option.empty,
      numberofRoomsSite = Option.empty,
      numberofStudentsSite = Option.empty,
      numberofEmployeesGroup = Option.empty,
      foodServedOnsite = Option.empty,
      conferenceOutletOnSite = Option.empty,
      twitterURL = Option.empty,
      facebookURL = Option.empty,
      instagramURL = Option.empty,
      numberofchildsites = Option.empty,
      lastlogintowebsite = Option.empty,
      lastnewsletteropened = Option.empty,
      subcribedtoUFSnewsletter = Option.empty,
      subcribedtoIceCreamnewsletter = Option.empty,
      last3onlineevents = Option.empty,
      facebookcampaignsclicked = Option.empty,
      liveChatactivities = Option.empty,
      loyaltyRewardsBalancePoints = Option.empty,
      loyaltyRewardsBalanceupdatedDate = Option.empty,
      accountSubType = Option.empty,
      visitorsPerYear = Option.empty,
      unileverNowClassification = Option.empty,
      tradingStatus = Option.empty,
      ctps = Option.empty,
      caterlystOpportunity = Option.empty,
      repAssessedOpportunity = Option.empty
    )
  }
}
