package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.constraint._

object Operator {
  val otmConstraint = FiniteDiscreteSetConstraint(Set("A", "B", "C", "D", "E", "F"))
}

case class Operator( // generic fields
    concatId: String,
    countryCode: String, // TODO Existing country code in OHUB using: Iso 3166-1 alpha 2
    isActive: Boolean,
    isGoldenRecord: Boolean,
    ohubId: Option[String],
    name: String,
    sourceEntityId: String,
    sourceName: String,
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp, // currently always created timestamp (how/when will it get an updated timestamp?)
    // specific fields
    averagePrice: Option[BigDecimal],
    chainId: Option[String],
    chainName: Option[String],
    channel: Option[String],
    city: Option[String],
    cookingConvenienceLevel: Option[String],
    countryName: Option[String], // TODO do a lookup for the countryName? that's derived why do we need it in the first place? so far it's only set for the file interface.
    customerType: Option[String], // TODO Options: entity types, why do we have this, isn't it encoded in the entity type implicitly?
    dateCreated: Option[Timestamp],
    dateUpdated: Option[Timestamp],
    daysOpen: Option[Int],
    distributorName: Option[String],
    distributorOperatorId: Option[String],
    emailAddress: Option[String],
    faxNumber: Option[String], // TODO set phone number constraint?
    hasDirectMailOptIn: Option[Boolean], // TODO can we merge the opt in & opt outs?
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
    kitchenType: Option[String], // TODO is this a finite discrete set? need to constraint it?
    mobileNumber: Option[String], // TODO set phone number constraint?
    netPromoterScore: Option[BigDecimal],
    oldIntegrationId: Option[String],
    otm: Option[String],
    otmEnteredBy: Option[String], // TODO is this also a finite discrete set? need to constraint it?
    phoneNumber: Option[String], // TODO set phone number constraint?
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

  emailAddress.foreach(EmailAddressConstraint.validate)
  daysOpen.foreach(NumberOfDaysConstraint.validate)
  weeksClosed.foreach(NumberOfWeeksConstraint.validate)

  otm.foreach(otmConstraint.validate)
}
