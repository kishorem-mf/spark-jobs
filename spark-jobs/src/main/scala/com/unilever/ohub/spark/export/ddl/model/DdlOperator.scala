package com.unilever.ohub.spark.export.ddl.model

import com.unilever.ohub.spark.export.DDLOutboundEntity
// scalastyle:off
case class DdlOperator(
                        id: String,
                        concatId: String,
                        countryCode: String,
                        customerType: String,
                        dateCreated: String,
                        dateUpdated: String,
                        accountStatus: String,
                        isGoldenRecord: String,
                        afhCustomerGoldenId: String,
                        customerName: String,
                        customerSAPConcatId: String,
                        sourceName: String,
                        ohubCreated: String,
                        ohubUpdated: String,
                        annualTurnover: String,
                        averagePricePerMeal: String,
                        averageRating: String,
                        beveragePurchasePotential: String,
                        buildingSquareFootage: String,
                        chainId: String,
                        chainName: String,
                        segment: String,
                        city: String,
                        convenienceLevel: String,
                        country: String,
                        weekOrYearOpen: String,
                        distributorName: String,
                        distributorOperatorId: String,
                        email: String,
                        fax: String,
                        hasDirectMailOptIn: String,
                        hasDirectMailOptOut: String,
                        hasEmailOptIn: String,
                        hasEmailOptOut: String,
                        hasFaxOptIn: String,
                        hasFaxOptOut: String,
                        hasGeneralOptOut: String,
                        hasMobileOptIn: String,
                        hasMobileOptOut: String,
                        hasTelemarketingOptIn: String,
                        hasTelemarketingOptOut: String,
                        headQuarterAddress: String,
                        headQuarterCity: String,
                        headQuarterPhoneNumber: String,
                        headQuarterState: String,
                        headQuarterZipCode: String,
                        houseNumber: String,
                        houseNumberExtension: String,
                        isNotRecalculatingOtm: String,
                        isOpenOnFriday: String,
                        dayWeekOpen: String,
                        isOpenOnSaturday: String,
                        isOpenOnSunday: String,
                        isOpenOnThursday: String,
                        isOpenOnTuesday: String,
                        isOpenOnWednesday: String,
                        isPrivateHousehold: String,
                        kitchenType: String,
                        menuKeywords: String,
                        mobileNumber: String,
                        netPromoterScore: String,
                        numberOfProductsFittingInMenu: String,
                        numberOfReviews: String,
                        oldIntegrationId: String,
                        operatorLeadScore: String,
                        otm: String,
                        otmReason: String,
                        phoneNumber1: String,
                        potentialSalesValue: String,
                        region: String,
                        salesRepresentative: String,
                        state: String,
                        addressLine1: String,
                        subSegments: String,
                        numberOfMealServedPerDay: String,
                        totalLocations: String,
                        numberOfEmployees: String,
                        vat: String,
                        wayOfServingAlcohol: String,
                        website: String,
                        webUpdaterId: String,
                        weeksClosed: String,
                        yearFounded: String,
                        postalCode: String,
                        localChannel: String,
                        channelUsage: String,
                        socialCommercial: String,
                        strategicChannel: String,
                        parentSegment: String,
                        globalSubChannel: String,
                        ufsClientNumber: String,
                        department: String,
                        crmAccountId: String,
                        channel: String,
                        division: String,
                        salesOrgId: String,
                        parentSourceCustomerCode: String,
                        closingTimeWorkingDay: String,
                        openingTimeWorkingDay: String,
                        preferredVisitDays: String,
                        preferredVisitStartTime: String,
                        preferredVisitEndTime: String,
                        preferredDeliveryDays: String,
                        preferredVisitNextMonth: String,
                        customerName2: String,
                        webshopRegistered: String,
                        loyaltyManagementOptIn: String,
                        foodSpendMonth: String,
                        latitude: String,
                        longitude: String,
                        customerHierarchyLevel3Desc: String,
                        customerHierarchyLevel4Desc: String,
                        customerHierarchyLevel5Desc: String,
                        mixedorUfs: String,
                        salesGroupKey: String,
                        salesOfficeKey: String,
                        eccIndustryKey: String,
                        salesDistrict: String,
                        customerGroup: String,
                        languageKey: String,
                        sapCustomerId: String,
                        recordType: String,
                        indirectAccount: String,
                        keyNumber: String,
                        customerHierarchyLevel7Desc: String,
                        hasWebshopAccount: String,
                        doYouHaveATerraceOrOutsideSeating: String,
                        doYouOfferTakeAways: String,
                        doYouOfferAHomeDeliveryService: String,
                        howManySeatsDoYouHave: String,
                        numberOfBeds: String,
                        numberOfRooms: String,
                        NumberOfStudents: String,
                        foodServedOnsite: String,
                        conferenceOutletOnSites: String,
                        twitterUrl: String,
                        facebookUrl: String,
                        instagramlUrl: String,
                        numberOfChildSites: String,
                        lastLoginToWebsite: String,
                        lastNewsLetterOpened: String,
                        subscribedToUfsNewsletter: String,
                        subscribedToIcecreamNewsletter: String,
                        last3OnlineEvents: String,
                        facebookCampaignsClicked: String,
                        livechatActivities: String,
                        loyaltyRewardsBalancePoints: String,
                        loyaltyRewardsBalanceUpdatedDate: String,
                        accountSubType: String,
                        visitorsPeryear: String,
                        unileverNowClassification: String,
                        tradingStatus: String,
                        ctps: String,
                        caterlystOpportunity: String,
                        repAssessedOpportunity: String,
                        preferredCommunicationMethod: String,
                        ssdPermitted: String,
                        telesales: String,
                        visitOk: String,
                        customerFlag: String,
                        mergeWith: String,
                        toBeMerged: String,
                        cpecialPrice: String,
                        categoryAttributes: String,
                        accountOwner: String,
                        numberOfTillPoints: String,
                        keyDecisionMaker: String,
                        seasonCloseTime: String,
                        poNumber: String,
                        otmOohCalculated: String,
                        otmUfsCalculated: String

                      ) extends DDLOutboundEntity
