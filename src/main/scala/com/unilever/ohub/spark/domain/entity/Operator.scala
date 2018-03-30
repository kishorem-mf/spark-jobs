package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.constraint._

case class Operator( // generic fields
                     countryCode: String, // Existing country code in OHUB using: Iso 3166-1 alpha 2
                     isActive: Boolean,
                     isGoldenRecord: Boolean,
                     groupId: Option[String],
                     name: String,
                     sourceEntityId: String,
                     sourceName: String,
                     // specific fields
                     averagePrice: Option[BigDecimal],
                     chainId: Option[String],
                     chainName: Option[String],
                     channel: Option[String],
                     city: Option[String],
                     cookingConvenienceLevel: Option[String],
                     countryName: Option[String],
                     customerType: Option[String],
                     dateCreated: Option[Timestamp],
                     dateUpdated: Option[Timestamp],
                     daysOpen: Option[Int],
                     distributorCustomerNumber: Option[String],
                     distributorName: Option[String],
                     distributorOperatorId: Option[String],
                     emailAddress: Option[String],
                     faxNumber: Option[String],
                     germanChainId: Option[String], // TODO move to additional fields (later)
                     germanChainName: Option[String], // TODO move to additional fields (later)
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
                     ohubCreated: Option[Timestamp],
                     ohubUpdated: Option[Timestamp],
                     oldIntegrationId: Option[String],
                     otm: Option[String], // Options: A | B | C | D | E | F
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
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity {
  emailAddress.foreach(EmailAddressConstraint.validate)
  daysOpen.foreach(NumberOfDaysConstraint.validate)
  weeksClosed.foreach(NumberOfWeeksConstraint.validate)
}
