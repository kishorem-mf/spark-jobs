package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.domain.DomainEntity

object TestOperators extends TestOperators

trait TestOperators {

  def defaultOperatorWithSourceName(sourceName: String): Operator =
    defaultOperator.copy(
      sourceName = sourceName,
      concatId = DomainEntity.createConcatIdFromValues(defaultOperator.countryCode, sourceName, defaultOperator.sourceEntityId)
    )

  def defaultOperatorWithSourceNameAndCountryCode(source: String, countryCode: String): Operator =
    defaultOperator.copy(
      sourceName = source,
      countryCode = countryCode,
      concatId = DomainEntity.createConcatIdFromValues(countryCode, source, defaultOperator.sourceEntityId)
    )

  def defaultOperatorWithSourceEntityId(sourceEntityId: String): Operator =
    defaultOperator.copy(
      sourceEntityId = sourceEntityId,
      concatId = DomainEntity.createConcatIdFromValues(defaultOperator.countryCode, defaultOperator.sourceName, sourceEntityId)
    )

  // format: OFF
  lazy val defaultOperator: Operator = Operator(
    id                          = "id-1",
    creationTimestamp           = new Timestamp(1542205922011L),
    concatId                    = "country-code~source-name~source-entity-id",
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
    averagePrice                = Some(BigDecimal(12345)),
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
    mobileNumber                = Some("+31612345678"),
    netPromoterScore            = Some(BigDecimal(75)),
    oldIntegrationId            = Some("old-integration-id"),
    otm                         = Some("D"),
    otmEnteredBy                = Some("otm-entered-by"),
    phoneNumber                 = Some("+31123456789"),
    region                      = Some("region"),
    salesRepresentative         = Some("sales-representative"),
    state                       = Some("state"),
    street                      = Some("street"),
    subChannel                  = Some("sub-channel"),
    totalDishes                 = Some(150),
    totalLocations              = Some(25),
    totalStaff                  = Some(105),
    vat                         = Some("vat"),
    webUpdaterId                = Some("web-updater-id"),
    weeksClosed                 = Some(2),
    zipCode                     = Some("1234 AB"),
    additionalFields            = Map(),
    ingestionErrors             = Map()
  )
  // format: ON
}
