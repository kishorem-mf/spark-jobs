package com.unilever.ohub.spark.domain

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

// TODO check order, formats, probably some of the fields can be pulled up to the domain entity
// TODO add domain entity validation
// TODO add additionalFields, etc.

// all booleans have: Y | N
// all strings are UTF-8 strings, except country code?

case class Operator(
                     sourceOperatorId: String,                                // TODO rename to sourceId
                     sourceName: String,                                      // UFT-8 characters: existing OHUB source name
                     countryCode: String,                                     // Existing country code in OHUB using: Iso 3166-1 alpha 2
                     isActive: Boolean,                                       // A | D
                     name: String,
                     oldIntegrationId: Option[String],                        // Must be a known operator integration id withing OHUB
                     customerConcatId: Option[String],                        // TODO rename to concatId? samenstelling van mandatory fields: countrycode ~ source ~ sourceOperaterId => thus mandatory itself,
                     webUpdaterId: Option[String],
                     customerType: Option[String],
                     dateCreated: Option[Timestamp],                          // YYYYMMDD HH24:MI:SS
                     dateUpdated: Option[Timestamp],                          // YYYYMMDD HH24:MI:SS
                     ohubCreated: Option[Timestamp],                          // YYYYMMDD HH24:MI:SS
                     ohubUpdated: Option[Timestamp],                          // YYYYMMDD HH24:MI:SS
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
                     emailAddress: Option[String],                            // Valid email adress format: xxxx@xxx.xxx
                     phoneNumber: Option[String],                             // International format: +xx x xxxx xxxx
                     mobileNumber: Option[String],                            // International format: +xx x xxxx xxxx
                     faxNumber: Option[String],                               // International format: +xx x xxxx xxxx
                     generalOptOut: Option[Boolean],                          // TODO these opts done follow our naming convention ('every boolean start with isXXX')
                     emailOptIn: Option[Boolean],
                     emailOptOut: Option[Boolean],
                     directMailOptIn: Option[Boolean],
                     directMailOptOut: Option[Boolean],
                     telemarketingOptIn: Option[Boolean],
                     telemarketingOptOut: Option[Boolean],
                     mobileOptIn: Option[Boolean],
                     mobileOptOut: Option[Boolean],
                     faxOptIn: Option[Boolean],
                     faxOptOut: Option[Boolean],
                     totalDishes: Option[Int],                                // Integer | Range of integers ("100-150") // TODO how to handle range
                     totalLocations: Option[Int],                             // Integer
                     totalStaff: Option[Int],                                 // Integer | Range of integers ("100-150") TODO how to handle range
                     averagePrice: Option[BigDecimal],                        // Decimal (decimal separator: ".") | Range of decimals ("29.95-39.95")
                     daysOpen: Option[Int],                                   // [0 - 7]
                     weeksClosed: Option[Int],                                // [0 - 52]
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
                     germanChainId: Option[String],                           // TODO move to additional fields
                     germanChainName: Option[String],
                     kitchenType: Option[String],
                     ingestionErrors: Map[String, IngestionError]
                   ) extends DomainEntity
