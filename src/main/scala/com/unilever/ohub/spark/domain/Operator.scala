package com.unilever.ohub.spark.domain

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.constraint._

// TODO check order, formats

case class Operator(
                     sourceEntityId: String,
                     sourceName: String,                                      // UFT-8 characters: existing OHUB source name
                     countryCode: String,                                     // Existing country code in OHUB using: Iso 3166-1 alpha 2
                     isActive: Boolean,
                     name: String,
                     oldIntegrationId: Option[String],                        // Must be a known operator integration id withing OHUB
                     webUpdaterId: Option[String],
                     customerType: Option[String],
                     dateCreated: Option[Timestamp],
                     dateUpdated: Option[Timestamp],
                     ohubCreated: Option[Timestamp],
                     ohubUpdated: Option[Timestamp],
                     channel: Option[String],
                     subChannel: Option[String],
                     region: Option[String],
                     street: Option[String],
                     houseNumber: Option[String],
                     houseNumberExtension: Option[String],
                     city: Option[String],
                     zipCode: Option[String],
                     state: Option[String],
                     countryName: Option[String],
                     emailAddress: Option[String],
                     phoneNumber: Option[String],                             // International format: +xx x xxxx xxxx
                     mobileNumber: Option[String],                            // International format: +xx x xxxx xxxx
                     faxNumber: Option[String],                               // International format: +xx x xxxx xxxx
                     hasGeneralOptOut: Option[Boolean],
                     hasEmailOptIn: Option[Boolean],
                     hasEmailOptOut: Option[Boolean],
                     hasDirectMailOptIn: Option[Boolean],
                     hasDirectMailOptOut: Option[Boolean],
                     hasTelemarketingOptIn: Option[Boolean],
                     hasTelemarketingOptOut: Option[Boolean],
                     hasMobileOptIn: Option[Boolean],
                     hasMobileOptOut: Option[Boolean],
                     hasFaxOptIn: Option[Boolean],
                     hasFaxOptOut: Option[Boolean],
                     totalDishes: Option[Int],
                     totalLocations: Option[Int],
                     totalStaff: Option[Int],
                     averagePrice: Option[BigDecimal],
                     daysOpen: Option[Int],
                     weeksClosed: Option[Int],
                     distributorName: Option[String],
                     distributorCustomerNumber: Option[String],
                     distributorOperatorId: Option[String],
                     otm: Option[String],                                     // Options: A | B | C | D | E | F
                     otmEnteredBy: Option[String],
                     isNotRecalculatingOtm: Option[Boolean],
                     netPromoterScore: Option[String],
                     salesRepresentative: Option[String],
                     cookingConvenienceLevel: Option[String],
                     isPrivateHousehold: Option[Boolean],
                     vat: Option[String],
                     isOpenOnMonday: Option[Boolean],
                     isOpenOnTuesday: Option[Boolean],
                     isOpenOnWednesday: Option[Boolean],
                     isOpenOnThursday: Option[Boolean],
                     isOpenOnFriday: Option[Boolean],
                     isOpenOnSaturday: Option[Boolean],
                     isOpenOnSunday: Option[Boolean],
                     chainName: Option[String],
                     chainId: Option[String],
                     germanChainId: Option[String],                           // TODO move to additional fields (later)
                     germanChainName: Option[String],
                     kitchenType: Option[String],
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity {
  emailAddress.foreach( EmailAddressConstraint.validate )
  daysOpen.foreach( NumberOfDaysConstraint.validate )
  weeksClosed.foreach( NumberOfWeeksConstraint.validate )
}
