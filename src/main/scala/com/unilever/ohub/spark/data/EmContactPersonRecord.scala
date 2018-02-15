package com.unilever.ohub.spark.data

import java.sql.Timestamp

  case class EmContactPersonRecord(
    contactPersonId: Option[Long],
    countryCode: Option[String],
    emSourceId: Option[String],
    webServiceRequestId: Option[Long],
    webupdaterId: Option[Long],
    emailAddress: Option[String],
    mobilePhone: Option[Long],
    phone: Option[Long],
    fax: Option[Long],
    title: Option[String], // enum: Mr., ...
    firstName: Option[String],
    lastName: Option[String],
    optIn: Option[Boolean], // 0/1
    jobTitle: Option[String], // enum // Option[Either[Long, String]]
    language: Option[String], // enum
    operatorName: Option[String],
    typeOfBusiness: Option[String], // enum
    street: Option[String],
    houseNumber: Option[String],
    houseNumberExt: Option[String], // empty
    postcode: Option[String],
    city: Option[String],
    state: Option[String], // empty
    country: Option[String], // enum
    typeOfCuisine: Option[String], // enum // Option[Either[Long, String]]
    nrOfCoversPerDay: Option[Long],
    nrOfLocations: Option[Long],
    nrOfKitchenStaff: Option[Long],
    primaryDistributor: Option[String],
    gender: Option[String], // enum: M/F
    operatorRefId: Option[Long], // empty
    optInDate: Option[Timestamp],
    confirmedOptIn: Option[Boolean], // 0/1
    confirmedOptInDate: Option[Timestamp],
    distributorCustomerId: Option[String],
    privateHousehold: Option[Boolean], // 0/1
    vat: Option[String], // ?, empty
    openOnMonday: Option[Boolean], // 0/1
    openOnTuesday: Option[Boolean], // 0/1
    openOnWednesday: Option[Boolean], // 0/1
    openOnThursday: Option[Boolean], // 0/1
    openOnFriday: Option[Boolean], // 0/1
    openOnSaturday: Option[Boolean], // 0/1
    openOnSunday: Option[Boolean]) // 0/1
