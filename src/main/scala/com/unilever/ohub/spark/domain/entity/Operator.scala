package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.constraint._

object Operator {
  val otmConstraint = FiniteDiscreteSetConstraint(Set("A", "B", "C", "D", "E", "F"))
  val customerType = "operator"
  val daysOpenRange = Range.inclusive(0, 7)
  val weeksClosedRange = Range.inclusive(0, 52)
}

case class Operator(
    // generic fields
    concatId: String,
    countryCode: String, // TODO Existing country code in OHUB using: Iso 3166-1 alpha 2...do a lookup
    customerType: String,
    dateCreated: Option[Timestamp],
    dateUpdated: Option[Timestamp],
    isActive: Boolean,
    isGoldenRecord: Boolean,
    ohubId: Option[String],
    name: String,
    sourceEntityId: String,
    sourceName: String, // TODO existing source, add constraint? do a lookup
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp, // currently always created timestamp (how/when will it get an updated timestamp?)
    // specific fields
    averagePrice: Option[BigDecimal],
    chainId: Option[String],
    chainName: Option[String],
    channel: Option[String],
    city: Option[String],
    cookingConvenienceLevel: Option[String],
    countryName: Option[String], // can be derived...however, having the country name right away has benefits.
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
) extends DomainEntity {
  import Operator._

  // TODO refine...what's the minimal amount of constraints needed before an operator should be accepted

  emailAddress.foreach(EmailAddressConstraint.validate)

  // days open en weeks closed
  daysOpen.foreach(NumberOfDaysConstraint.validate)
  weeksClosed.foreach(NumberOfWeeksConstraint.validate)

  otm.foreach(otmConstraint.validate)
}
