package com.unilever.ohub.spark.data

import java.sql.Timestamp

case class OperatorRecord(
  operatorConcatId: String, refOperatorId: Option[String], source: Option[String],
  countryCode: Option[String], status: Option[Boolean], statusOriginal: Option[String],
  name: Option[String], nameCleansed: Option[String], operatorIntegrationId: Option[String],
  dateCreated: Option[Timestamp], dateModified: Option[Timestamp], channel: Option[String],
  subChannel: Option[String], region: Option[String],
  street: Option[String], streetCleansed: Option[String], housenumber: Option[String],
  housenumberExt: Option[String], city: Option[String], cityCleansed: Option[String],
  zipCode: Option[String], zipCodeCleansed: Option[String], state: Option[String],
  country: Option[String], emailAddress: Option[String], phoneNumber: Option[String],
  mobilePhoneNumber: Option[String], faxNumber: Option[String],
  optOut: Option[Boolean], emailOptIn: Option[Boolean], emailOptOut: Option[Boolean],
  directMailOptIn: Option[Boolean], directMailOptOut: Option[Boolean],
  telemarketingOptIn: Option[Boolean], telemarketingOptOut: Option[Boolean], mobileOptIn: Option[Boolean],
  mobileOptOut: Option[Boolean],
  faxOptIn: Option[Boolean], faxOptOut: Option[Boolean], nrOfDishes: Option[Long],
  nrOfDishesOriginal: Option[String], nrOfLocations: Option[String], nrOfStaff: Option[String],
  avgPrice: Option[BigDecimal], avgPriceOriginal: Option[String],
  daysOpen: Option[Long], daysOpenOriginal: Option[String], weeksClosed: Option[Long],
  weeksClosedOriginal: Option[String], distributorName: Option[String],
  distributorCustomerNr: Option[String], otm: Option[String],
  otmReason: Option[String], otmDnr: Option[Boolean], otmDnrOriginal: Option[String],
  npsPotential: Option[BigDecimal], npsPotentialOriginal: Option[String], salesRep: Option[String],
  convenienceLevel: Option[String],
  privateHousehold: Option[Boolean], privateHouseholdOriginal: Option[String],
  vatNumber: Option[String], openOnMonday: Option[Boolean], openOnMondayOriginal: Option[String],
  openOnTuesday: Option[Boolean], openOnTuesdayOriginal: Option[String],
  openOnWednesday: Option[Boolean], openOnWednesdayOriginal: Option[String],
  openOnThursday: Option[Boolean], openOnThursdayOriginal: Option[String],
  openOnFriday: Option[Boolean], openOnFirdayOriginal: Option[String],
  openOnSaturday: Option[Boolean], openOnSaturdayOriginal: Option[String],
  openOnSunday: Option[Boolean], openOnSundayOriginal: Option[String], chainName: Option[String],
  chainId: Option[String], kitchenType: Option[String]
)
