package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.domain.DomainEntity

object TestOperators extends TestOperators

trait TestOperators {

  def defaultOperatorWithSourceName(sourceName: String): Operator =
    defaultOperator.copy(
      sourceName = sourceName,
      concatId = Util.createConcatIdFromValues(defaultOperator.countryCode, sourceName, defaultOperator.sourceEntityId)
    )

  def defaultOperatorWithSourceNameAndCountryCode(source: String, countryCode: String): Operator =
    defaultOperator.copy(
      sourceName = source,
      countryCode = countryCode,
      concatId = Util.createConcatIdFromValues(countryCode, source, defaultOperator.sourceEntityId)
    )

  def defaultOperatorWithSourceEntityId(sourceEntityId: String): Operator =
    defaultOperator.copy(
      sourceEntityId = sourceEntityId,
      concatId = Util.createConcatIdFromValues(defaultOperator.countryCode, defaultOperator.sourceName, sourceEntityId)
    )

  // format: OFF
  lazy val defaultOperator: Operator = Operator(
    id                          = "id-1",
    creationTimestamp           = new Timestamp(1542205922011L),
    concatId                    = "country-code~EMAKINA-name~source-entity-id",
    countryCode                 = "country-code",
    customerType                = Operator.customerType,
    dateCreated                 = Some(Timestamp.valueOf("2017-10-16 18:09:49")),
    dateUpdated                 = Some(Timestamp.valueOf("2017-10-16 18:09:49")),
    isActive                    = true,
    isGoldenRecord              = false,
    ohubId                      = Some(UUID.randomUUID().toString),
    name                        = Some("operator-name"),
    sourceEntityId              = "source-entity-id",
    sourceName                  = "source-name",
    ohubCreated                 = Timestamp.valueOf("2017-10-16 18:09:49"),
    ohubUpdated                 = Timestamp.valueOf("2017-10-16 18:09:49"),
    annualTurnover              = Some(BigDecimal("21312312312312")),
    averagePrice                = Some(BigDecimal(12345)),
    averageRating               = Some(3),
    beveragePurchasePotential   = Some(BigDecimal(3444444)),
    buildingSquareFootage       = Some("1234m2"),
    chainId                     = Some("chain-id"),
    chainName                   = Some("chain-name"),
    channel                     = Some("channel"),
    city                        = Some("city"),
    cookingConvenienceLevel     = Some("cooking-convenience-level"),
    countryName                 = Some("country-name"),
    daysOpen                    = Some(4),
    distributorName             = Some("distributor-name"),
    distributorOperatorId       = None,
    emailAddress                = Some("email-address@some-server.com"),
    faxNumber                   = Some("+31123456789"),
    hasDirectMailOptIn          = Some(true),
    hasDirectMailOptOut         = Some(false),
    hasEmailOptIn               = Some(true),
    hasEmailOptOut              = Some(false),
    hasFaxOptIn                 = Some(true),
    hasFaxOptOut                = Some(false),
    hasGeneralOptOut            = Some(false),
    hasMobileOptIn              = Some(true),
    hasMobileOptOut             = Some(false),
    headQuarterAddress          = Some("Street 12"),
    headQuarterCity             = Some("Amsterdam"),
    headQuarterPhoneNumber      = Some("+31 2131231"),
    headQuarterState            = Some("North Holland"),
    headQuarterZipCode          = Some("1111AA"),
    hasTelemarketingOptIn       = Some(true),
    hasTelemarketingOptOut      = Some(false),
    houseNumber                 = Some("12"),
    houseNumberExtension        = None,
    isNotRecalculatingOtm       = Some(true),
    isOpenOnFriday              = Some(true),
    isOpenOnMonday              = Some(false),
    isOpenOnSaturday            = Some(true),
    isOpenOnSunday              = Some(false),
    isOpenOnThursday            = Some(true),
    isOpenOnTuesday             = Some(true),
    isOpenOnWednesday           = Some(true),
    isPrivateHousehold          = Some(false),
    kitchenType                 = Some("kitchen-type"),
    menuKeywords                = Some("meat, fish"),
    mobileNumber                = Some("+31612345678"),
    netPromoterScore            = Some(BigDecimal(75)),
    numberOfProductsFittingInMenu = Some(1),
    numberOfReviews             = Some(3),
    oldIntegrationId            = None,
    operatorLeadScore           = Some(1),
    otm                         = Some("D"),
    otmEnteredBy                = Some("otm-entered-by"),
    phoneNumber                 = Some("+31123456789"),
    potentialSalesValue         = Some(BigDecimal(123)),
    region                      = Some("region"),
    salesRepresentative         = Some("sales-representative"),
    state                       = Some("state"),
    street                      = Some("street"),
    subChannel                  = Some("sub-channel"),
    totalDishes                 = Some(150),
    totalLocations              = Some(25),
    totalStaff                  = Some(105),
    vat                         = Some("vat"),
    wayOfServingAlcohol         = Some("None"),
    website                     = Some("www.google.com"),
    webUpdaterId                = Some("web-updater-id"),
    weeksClosed                 = Some(2),
    yearFounded                 = Some(1900),
    zipCode                     = Some("1234 AB"),
    localChannel                = None,
    channelUsage                = None,
    socialCommercial            = None,
    strategicChannel            = None,
    globalChannel               = None,
    globalSubChannel            = None,
    ufsClientNumber             = Some("ufsClientNumber"),
    additionalFields            = Map(),
    ingestionErrors             = Map()
  )
  // format: ON
}
