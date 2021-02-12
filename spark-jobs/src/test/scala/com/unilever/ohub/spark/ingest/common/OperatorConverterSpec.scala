package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class OperatorConverterSpec extends CsvDomainGateKeeperSpec[Operator] {

  override val SUT = OperatorConverter

  describe("common operator converter") {
    it("should convert an operator correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_OPERATORS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 6

        import com.unilever.ohub.spark.SharedSparkSession.spark
        import spark.implicits._

        val actualOperator = actualDataSet.filter($"id" === "id-3").head()

        val expectedOperator = Operator(
          id = "id-3",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "TR~KANGAROO~HG_226466866",
          countryCode = "TR",
          customerType = "OPERATOR",
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:47:37")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:47:37")),
          isActive = true,
          isGoldenRecord = false,
          name = Some("Kebapçim Ali - Bodrum"),
          sourceName = "KANGAROO",
          sourceEntityId = "HG_226466866",
          ohubId = actualOperator.ohubId,
          ohubCreated = actualOperator.ohubCreated,
          ohubUpdated = actualOperator.ohubUpdated,
          annualTurnover = None,
          averagePrice = None,
          averageRating = None,
          beveragePurchasePotential = None,
          buildingSquareFootage = None,
          chainId = Some("UG_1"),
          chainName = Some("Diger Müsteriler"),
          channel = Some("Restaurants"),
          city = Some("Mugla"),
          cookingConvenienceLevel = None,
          countryName = Some("Turkey"),
          daysOpen = None,
          distributorName = None,
          distributorOperatorId = None,
          emailAddress = None,
          faxNumber = None,
          hasDirectMailOptIn = Some(true),
          hasDirectMailOptOut = None,
          hasEmailOptIn = Some(true),
          hasEmailOptOut = None,
          hasFaxOptIn = Some(true),
          hasFaxOptOut = None,
          hasGeneralOptOut = None,
          hasMobileOptIn = Some(true),
          hasMobileOptOut = None,
          hasTelemarketingOptIn = Some(true),
          hasTelemarketingOptOut = None,
          headQuarterAddress = None,
          headQuarterCity = None,
          headQuarterPhoneNumber = None,
          headQuarterState = None,
          headQuarterZipCode = None,
          houseNumber = None,
          houseNumberExtension = None,
          isNotRecalculatingOtm = None,
          isOpenOnFriday = Some(true),
          isOpenOnMonday = Some(true),
          isOpenOnSaturday = Some(true),
          isOpenOnSunday = None,
          isOpenOnThursday = Some(true),
          isOpenOnTuesday = Some(true),
          isOpenOnWednesday = Some(true),
          isPrivateHousehold = None,
          kitchenType = None,
          menuKeywords = None,
          mobileNumber = Some("05356800669"),
          netPromoterScore = Some(BigDecimal("0")),
          numberOfProductsFittingInMenu = None,
          numberOfReviews = None,
          oldIntegrationId = None,
          operatorLeadScore = None,
          otm = None,
          otmEnteredBy = None,
          phoneNumber = Some("0252 3854478"),
          potentialSalesValue = None,
          region = Some("Mugla"),
          salesRepresentative = Some("Bodrum (Vacant)"),
          state = Some("Bodrum"),
          street = Some("Yalikavak Mahallesi, Çökertme Caddesi, No 187, Bodrum, Mugla"),
          subChannel = Some("Kebapçi"),
          totalDishes = Some(120),
          totalLocations = None,
          totalStaff = None,
          vat = Some("18808137040"),
          wayOfServingAlcohol = None,
          website = None,
          webUpdaterId = None,
          weeksClosed = None,
          yearFounded = None,
          zipCode = None,
          localChannel = None,
          channelUsage = None,
          socialCommercial = None,
          strategicChannel = None,
          globalChannel = None,
          globalSubChannel = None,
          //CRM Fields
          crmId=None,
          distributionChannel=None,
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
          parentChannel=None,
          accountType=None,
          accountStatus=None,
          hasWebshopRegistration=None,
          hasLoyaltyManagementOptIn=None,
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
          sapCustomerId=None,
          recordType=None,
          isIndirectAccount=None,
          keyNumber=None,
          hasWebshopAccount=None,
          daysOpenPerWeek=None,
          hasOutsideSeatings=None,
          hasTakeAway=None,
          hasHomeDelivery=None,
          numberOfSeats=None,
          numberOfBedsRange=None,
          numberOfRoomsRange=None,
          numberOfStudentsRange=None,
          totalStaffRange=None,
          hasFoodOnsite=None,
          hasConference=None,
          twitterUrl=None,
          facebookUrl=None,
          instagramUrl=None,
          numberOfChildSites=None,
          lastWebLoginDate=None,
          lastOpenedNewsletterDate=None,
          subcribedToUfsNewsletter=None,
          subcribedToIceCreamNewsletter=None,
          last3OnlineEvents=None,
          totalFacebookCampaignsClicked=None,
          livechatActivities=None,
          totalLoyaltyRewardsBalancePoints=None,
          loyaltyRewardsBalanceUpdatedDate=None,
          accountSubType=None,
          visitorsPerYear=None,
          unileverNowClassification=None,
          tradingStatus=None,
          hasTelephoneSuppressed=None,
          caterlystPotentialTurnover=None,
          salesRepEstimatedPotentialTurnover=None,
          preferredCommunicationMethod=None,
          hasPermittedToShareSsd=None,
          isOkWithVisits=None,
          customerFlag=None,
          mergeWith=None,
          hasToBeMerged=None,
          specialPrice=None,
          categoryAttributes=None,
          accountOwner=None,
          numberOfTillPoints=None,
          keyDecisionMaker=None,
          seasonCloseTime=None,
          poNumber=None,
          otmOohCalculated=None,
          otmUfsCalculated=None,
          eOtm=None,
          relativeLeadScore=None,
          hasPermittedToOrder=None,
          ofsValue=None,
          routeToMarketIceCreamCategory=None,
          //other fields
          additionalFields = Map(),
          ingestionErrors = Map(),
          ufsClientNumber = None,
          department = Some("UFS")
//          department = None
        )

        actualOperator shouldBe expectedOperator
      }
    }
  }
}
