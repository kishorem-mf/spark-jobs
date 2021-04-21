package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.domain.entity.OperatorGolden
import com.unilever.ohub.spark.storage.Storage
import com.unilever.ohub.spark.DefaultConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{ Window, WindowSpec }
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

object OperatorCreatePerfectGoldenRecord extends BaseMerging[OperatorGolden] {

  override def run(spark: SparkSession, config: DefaultConfig, storage: Storage): Unit = {
    val entity = storage.readFromParquet[OperatorGolden](config.inputFile)
    val entityAllCountries = entity
      .filter(!col("countryCode").isin("CH", "AT", "DE", "GB", "IE"))
    val entityDachCountries = entity
      .filter(col("countryCode").isin("CH", "AT", "DE"))
    val entityUkiCountries = entity
      .filter(col("countryCode").isin("GB", "IE"))

    val groupWin = Window.partitionBy(col("ohubId"))

    val orderByWindow = groupWin.orderBy(
      col("sourcePriority").desc_nulls_last,
      when(col("dateUpdated").isNull, col("dateCreated"))
        .otherwise(col("dateUpdated")).desc_nulls_last,
      col("dateCreated").desc_nulls_last,
      col("ohubUpdated").desc
    )
    val nonPreferredCol=Seq("id", "creationTimestamp", "concatId", "countryCode", "customerType", "dateCreated",
      "dateUpdated", "isGoldenRecord", "ohubId", "sourceEntityId", "sourceName", "ohubCreated", "ohubUpdated",
      "annualTurnover", "averagePrice", "averageRating", "beveragePurchasePotential", "buildingSquareFootage",
      "chainId", "chainName", "cookingConvenienceLevel", "countryName", "daysOpen", "distributorName",
      "distributorOperatorId", "hasDirectMailOptIn", "hasDirectMailOptOut", "hasEmailOptIn", "hasEmailOptOut",
      "hasFaxOptIn", "hasFaxOptOut", "hasGeneralOptOut", "hasMobileOptIn", "hasMobileOptOut", "hasTelemarketingOptIn",
      "hasTelemarketingOptOut", "headQuarterAddress", "headQuarterCity", "headQuarterPhoneNumber", "headQuarterState",
      "headQuarterZipCode", "houseNumberExtension", "isNotRecalculatingOtm", "isOpenOnFriday", "isOpenOnMonday",
      "isOpenOnSaturday", "isOpenOnSunday", "isOpenOnThursday", "isOpenOnTuesday", "isOpenOnWednesday",
      "isPrivateHousehold", "kitchenType", "menuKeywords", "netPromoterScore", "numberOfProductsFittingInMenu",
      "numberOfReviews", "oldIntegrationId", "operatorLeadScore", "otm", "otmEnteredBy", "potentialSalesValue",
      "salesRepresentative", "totalDishes", "totalLocations", "totalStaff", "vat", "wayOfServingAlcohol", "webUpdaterId",
      "weeksClosed", "yearFounded", "localChannel", "channelUsage", "socialCommercial", "strategicChannel", "globalChannel",
      "globalSubChannel", "ufsClientNumber", "department", "crmId", "distributionChannel", "division", "salesOrgId",
      "closingTimeWorkingDay", "openingTimeWorkingDay", "preferredVisitDays", "preferredVisitStartTime",
      "preferredVisitEndTime", "preferredDeliveryDays", "preferredVisitWeekOfMonth", "hasWebshopRegistration",
      "hasLoyaltyManagementOptIn", "monthlyFoodSpend", "customerHierarchyLevel3", "customerHierarchyLevel4", "customerHierarchyLevel5",
      "mixedOrUfs", "salesGroupKey", "salesOfficeKey", "industryKey", "salesDistrict", "customerGroup", "languageKey",
      "sapCustomerId", "isIndirectAccount", "hasWebshopAccount", "daysOpenPerWeek", "hasOutsideSeatings", "hasTakeAway",
      "hasHomeDelivery", "numberOfSeats", "twitterUrl", "facebookUrl", "instagramUrl", "numberOfChildSites", "lastWebLoginDate",
      "lastOpenedNewsletterDate", "subcribedToUfsNewsletter", "subcribedToIceCreamNewsletter", "last3OnlineEvents",
      "totalFacebookCampaignsClicked", "livechatActivities", "totalLoyaltyRewardsBalancePoints", "loyaltyRewardsBalanceUpdatedDate",
      "unileverNowClassification", "hasTelephoneSuppressed", "caterlystPotentialTurnover", "salesRepEstimatedPotentialTurnover",
      "preferredCommunicationMethod", "hasPermittedToShareSsd", "isOkWithVisits", "customerFlag", "mergeWith", "hasToBeMerged",
      "specialPrice", "categoryAttributes", "accountOwner", "numberOfTillPoints", "keyDecisionMaker", "seasonCloseTime", "poNumber",
      "otmOohCalculated", "otmUfsCalculated", "eOtm", "relativeLeadScore", "hasPermittedToOrder", "ofsValue", "routeToMarketIceCreamCategory")

    val dach_sources = List("CRM", "FUZZIT", "SAP_SIRIUS", "CONSESSIONAIRES_AT", "ECCO_JAEGER", "FRIGEMO", "GMUR", "GRUENENFELDER",
                            "MULTIFOOD", "VERMO", "WALKER", "SWISS_GATEWAY")
    val uki_sources = List("CRM", "CATERLYST", "SAP_SIRIUS")

    val outputGoldenDach=transform(spark,entityDachCountries,orderByWindow,dach_sources,nonPreferredCol)
    val outputGoldenUki=transform(spark,entityUkiCountries,orderByWindow,uki_sources,nonPreferredCol)
    val outputGoldenOthers=transform(spark,entityAllCountries)

    val dachUkiGolden=outputGoldenDach.unionByName(outputGoldenUki)
    val allDataGolden=dachUkiGolden.unionByName(outputGoldenOthers)

    storage.writeToParquet(allDataGolden, config.outputFile)
  }
}
