package com.unilever.ohub.spark.domain.entity

import java.sql.{Date, Timestamp}
import java.util.UUID

object TestContactPersonsGolden extends TestContactPersonsGolden

trait TestContactPersonsGolden {

  def defaultContactPersonWithSourceEntityId(sourceEntityId: String): ContactPersonGolden =
    defaultContactPersonGolden.copy(
      sourceEntityId = sourceEntityId,
      concatId = Util.createConcatIdFromValues(defaultContactPersonGolden.countryCode, defaultContactPersonGolden.sourceName, sourceEntityId)
    )

  val defaultContactPersonGolden = ContactPersonGolden(
    id = "id-1",
    creationTimestamp = new Timestamp(1542205922011L),
    concatId = "AU~WUFOO~AB123",
    countryCode = "AU",
    customerType = "CONTACTPERSON",
    dateCreated = Some(Timestamp.valueOf("2015-06-30 13:47:00.0")),
    dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:48:00.0")),
    isActive = true,
    isGoldenRecord = false,
    ohubId = Some(UUID.randomUUID().toString),
    sourceEntityId = "AB123",
    sourceName = "WUFOO",
    ohubCreated = Timestamp.valueOf("2015-06-30 13:49:00.0"),
    ohubUpdated = Timestamp.valueOf("2015-06-30 13:50:00.0"),
    operatorConcatId = Some("AU~WUFOO~E1-1234"),
    operatorOhubId = Some("operator-ohub-id"),
    oldIntegrationId = Some("G1234"),
    firstName = Some("John"),
    lastName = Some("Williams"),
    title = Some("Mr"),
    gender = Some("M"),
    jobTitle = Some("Chef"),
    language = Some("en"),
    birthDate = Some(Date.valueOf("1975-12-21")),
    street = Some("Highstreet"),
    houseNumber = Some("443"),
    houseNumberExtension = Some("A"),
    city = Some("Melbourne"),
    zipCode = Some("2057"),
    state = Some("Alabama"),
    countryName = Some("Australia"),
    isPreferredContact = Some(true),
    isKeyDecisionMaker = Some(true),
    standardCommunicationChannel = Some("Mobile"),
    emailAddress = Some("jwilliams@downunder.au"),
    phoneNumber = Some("61396621811"),
    mobileNumber = Some("61612345678"),
    faxNumber = Some("61396621812"),
    hasGeneralOptOut = Some(true),
    hasRegistration = Some(true),
    registrationDate = Some(Timestamp.valueOf("2015-09-30 14:23:01.0")),
    hasConfirmedRegistration = Some(true),
    confirmedRegistrationDate = Some(Timestamp.valueOf("2015-09-30 14:23:01.0")),
    hasEmailOptIn = Some(true),
    emailOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:02.0")),
    hasEmailDoubleOptIn = Some(true),
    emailDoubleOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:03.0")),
    hasEmailOptOut = Some(true),
    hasDirectMailOptIn = Some(true),
    hasDirectMailOptOut = Some(true),
    hasTeleMarketingOptIn = Some(true),
    hasTeleMarketingOptOut = Some(true),
    hasMobileOptIn = Some(true),
    mobileOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:04.0")),
    hasMobileDoubleOptIn = Some(true),
    mobileDoubleOptInDate = Some(Timestamp.valueOf("2015-09-30 14:23:05.0")),
    hasMobileOptOut = Some(true),
    hasFaxOptIn = Some(true),
    hasFaxOptOut = Some(true),
    webUpdaterId = None,
    isEmailAddressValid = None,
    isMobileNumberValid = None,
    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
