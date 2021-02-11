package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{DomainTransformer, OperatorEmptyParquetWriter}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.Decimal

object OperatorConverter extends CommonDomainGateKeeper[Operator] with OperatorEmptyParquetWriter {

  // scalastyle:off method.length
  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Operator = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      Operator(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        customerType = Operator.customerType,
        isActive = mandatory("isActive", toBoolean),
        isGoldenRecord = false,
        ohubId = Option.empty,
        name = optional("name"),
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        annualTurnover = optional("annualTurnover", toBigDecimal),
        averagePrice = optional("averagePrice", toBigDecimal),
        averageRating = optional("averageRating", toInt), //rating for 1 - 5
        beveragePurchasePotential = optional("beveragePurchasePotential", toBigDecimal),
        buildingSquareFootage = optional("buildingSquareFootage"),
        chainId = optional("chainId"),
        chainName = optional("chainName"),
        channel = optional("channel"),
        city = optional("city"),
        cookingConvenienceLevel = optional("cookingConvenienceLevel"),
        countryName = optional("countryName"),
        daysOpen = optional("daysOpen", toInt),
        distributorName = optional("distributorName"),
        distributorOperatorId = optional("distributorOperatorId"),
        emailAddress = optional("emailAddress"),
        faxNumber = optional("faxNumber"),
        hasDirectMailOptIn = optional("hasDirectMailOptIn", toBoolean),
        hasDirectMailOptOut = optional("hasDirectMailOptOut", toBoolean),
        hasEmailOptIn = optional("hasEmailOptIn", toBoolean),
        hasEmailOptOut = optional("hasEmailOptOut", toBoolean),
        hasFaxOptIn = optional("hasFaxOptIn", toBoolean),
        hasFaxOptOut = optional("hasFaxOptOut", toBoolean),
        hasGeneralOptOut = optional("hasGeneralOptOut", toBoolean),
        hasMobileOptIn = optional("hasMobileOptIn", toBoolean),
        hasMobileOptOut = optional("hasMobileOptOut", toBoolean),
        hasTelemarketingOptIn = optional("hasTelemarketingOptIn", toBoolean),
        hasTelemarketingOptOut = optional("hasTelemarketingOptOut", toBoolean),
        headQuarterAddress = optional("headQuarterAddress"),
        headQuarterCity = optional("headQuarterCity"),
        headQuarterPhoneNumber = optional("headQuarterPhoneNumber"),
        headQuarterState = optional("headQuarterState"),
        headQuarterZipCode = optional("headQuarterZipCode"),
        houseNumber = optional("houseNumber"),
        houseNumberExtension = optional("houseNumberExtension"),
        isNotRecalculatingOtm = optional("isNotRecalculatingOtm", toBoolean),
        isOpenOnFriday = optional("isOpenOnFriday", toBoolean),
        isOpenOnMonday = optional("isOpenOnMonday", toBoolean),
        isOpenOnSaturday = optional("isOpenOnSaturday", toBoolean),
        isOpenOnSunday = optional("isOpenOnSunday", toBoolean),
        isOpenOnThursday = optional("isOpenOnThursday", toBoolean),
        isOpenOnTuesday = optional("isOpenOnTuesday", toBoolean),
        isOpenOnWednesday = optional("isOpenOnWednesday", toBoolean),
        isPrivateHousehold = optional("isPrivateHousehold", toBoolean),
        kitchenType = optional("kitchenType"),
        menuKeywords = optional("menuKeywords"),
        mobileNumber = optional("mobileNumber"),
        netPromoterScore = optional("netPromoterScore", toBigDecimal),
        numberOfProductsFittingInMenu = optional("numberOfProductsFittingInMenu", toInt),
        numberOfReviews = optional("numberOfReviews", toInt),
        oldIntegrationId = optional("oldIntegrationId"),
        operatorLeadScore = optional("operatorLeadScore", toInt),
        otm = optional("otm"),
        otmEnteredBy = optional("otmEnteredBy"),
        phoneNumber = optional("phoneNumber"),
        potentialSalesValue = optional("potentialSalesValue", toBigDecimal),
        region = optional("region"),
        salesRepresentative = optional("salesRepresentative"),
        state = optional("state"),
        street = optional("street"),
        subChannel = optional("subChannel"),
        totalDishes = optional("totalDishes", toInt),
        totalLocations = optional("totalLocations", toInt),
        totalStaff = optional("totalStaff", toInt),
        vat = optional("vat"),
        wayOfServingAlcohol = optional("wayOfServingAlcohol"),
        website = optional("website"),
        webUpdaterId = None,
        weeksClosed = optional("weeksClosed", toInt),
        yearFounded = optional("yearFounded", toInt),
        zipCode = optional("zipCode"),
        localChannel = None,
        channelUsage = None,
        socialCommercial = None,
        strategicChannel = None,
        globalChannel = None,
        globalSubChannel = None,
        ufsClientNumber = optional("ufsClientNumber"),
        department = if(!optional("department").isDefined){Some("UFS")} else { optional("department")},
        //CRM fields
        crmAccountId=None,
        division=None,
        salesOrgId=None,
        parentSourceCustomerCode=None,
        closingTimeWorkingDay=None,
        openingTimeWorkingDay=None,
        preferredVisitDays=None,
        preferredVisitStartTime=None,
        preferredVisitEndTime=None,
        preferredDeliveryDays=None,
        preferredVisitWeekOfMonth=None,
        name2=None,
        accountType=None,
        monthlyFoodSpend=None,
        latitude=None,
        longitude=None,
        customerHierarchyLevel3=None,
        customerHierarchyLevel4=None,
        customerHierarchyLevel5=None,
        customerHierarchyLevel7=None,
        mixedOrUfs=None,
        salesGroupKey=None,
        salesOfficeKey=None,
        industryKey=None,
        salesDistrict=None,
        customerGroup=None,
        languageKey=None,
        recordType=None,
        isIndirectAccount=None,
        keyNumber=None,
        hasOutsideSeatings=None,
        hasTakeAway=None,
        hasHomeDelivery=None,
        numberOfSeats=None,
        numberOfBedsRange=None,
        numberOfRoomsRange=None,
        numberOfStudentsRange=None,
        hasFoodOnsite=None,
        hasConference=None,
        twitterUrl=None,
        facebookUrl=None,
        instagramUrl=None,
        numberOfChildSites=None,
        totalFacebookCampaignsClicked=None,
        accountSubType=None,
        visitorsPerYear=None,
        unileverNowClassification=None,
        tradingStatus=None,
        hasTelephoneSuppressed=None,
        caterlystPotentialTurnover=None,
        salesRepEstimatedPotentialTurnover=None,
        preferredCommunicationMethod=None,
        hasPermittedToShareSsd=None,
        otmOohCalculated=None,
        otmUfsCalculated=None,

        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
