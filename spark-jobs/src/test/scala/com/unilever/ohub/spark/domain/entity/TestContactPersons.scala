package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp
import java.util.UUID

import com.unilever.ohub.spark.domain.DomainEntity

object TestContactPersons extends TestContactPersons

trait TestContactPersons {

  def defaultContactPersonWithSourceEntityId(sourceEntityId: String): ContactPerson =
    defaultContactPerson.copy(
      sourceEntityId = sourceEntityId,
      concatId = DomainEntity.createConcatIdFromValues(defaultContactPerson.countryCode, defaultContactPerson.sourceName, sourceEntityId)
    )

  val defaultContactPerson = ContactPerson(
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
    operatorConcatId = "AU~WUFOO~E1-1234",
    operatorOhubId = Some("operator-ohub-id"),
    oldIntegrationId = Some("G1234"),
    firstName = Some("John"),
    lastName = Some("Williams"),
    title = Some("Mr"),
    gender = Some("M"),
    jobTitle = Some("Chef"),
    language = Some("en"),
    birthDate = Some(Timestamp.valueOf("1975-12-21 00:00:00.0")),
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
    additionalFields = Map(),
    ingestionErrors = Map()
  )
}
