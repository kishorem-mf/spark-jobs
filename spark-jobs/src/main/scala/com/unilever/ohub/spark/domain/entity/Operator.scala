package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object Operator {
  val customerType = "OPERATOR"
}

case class Operator(
    // generic fields
    concatId: String,
    countryCode: String,
    customerType: String,
    dateCreated: Option[Timestamp],
    dateUpdated: Option[Timestamp],
    isActive: Boolean,
    isGoldenRecord: Boolean,
    ohubId: Option[String],
    name: String,
    sourceEntityId: String,
    sourceName: String,
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp,
    // specific fields
    averagePrice: Option[BigDecimal],
    chainId: Option[String],
    chainName: Option[String],
    channel: Option[String],
    city: Option[String],
    cookingConvenienceLevel: Option[String],
    countryName: Option[String],
    daysOpen: Option[Int],
    distributorName: Option[String],
    distributorOperatorId: Option[String],
    emailAddress: Option[String],
    faxNumber: Option[String],
    hasDirectMailOptIn: Option[Boolean],
    hasDirectMailOptOut: Option[Boolean],
    hasEmailOptIn: Option[Boolean],
    hasEmailOptOut: Option[Boolean],
    hasFaxOptIn: Option[Boolean],
    hasFaxOptOut: Option[Boolean],
    hasGeneralOptOut: Option[Boolean],
    hasMobileOptIn: Option[Boolean],
    hasMobileOptOut: Option[Boolean],
    hasTelemarketingOptIn: Option[Boolean],
    hasTelemarketingOptOut: Option[Boolean],
    houseNumber: Option[String],
    houseNumberExtension: Option[String],
    isNotRecalculatingOtm: Option[Boolean],
    isOpenOnFriday: Option[Boolean],
    isOpenOnMonday: Option[Boolean],
    isOpenOnSaturday: Option[Boolean],
    isOpenOnSunday: Option[Boolean],
    isOpenOnThursday: Option[Boolean],
    isOpenOnTuesday: Option[Boolean],
    isOpenOnWednesday: Option[Boolean],
    isPrivateHousehold: Option[Boolean],
    kitchenType: Option[String],
    mobileNumber: Option[String],
    netPromoterScore: Option[BigDecimal],
    oldIntegrationId: Option[String],
    otm: Option[String],
    otmEnteredBy: Option[String],
    phoneNumber: Option[String],
    region: Option[String],
    salesRepresentative: Option[String],
    state: Option[String],
    street: Option[String],
    subChannel: Option[String],
    totalDishes: Option[Int],
    totalLocations: Option[Int],
    totalStaff: Option[Int],
    vat: Option[String],
    webUpdaterId: Option[String],
    weeksClosed: Option[Int],
    zipCode: Option[String],
    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity