package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object ContactPerson {
  val customerType = "CONTACTPERSON"
}

case class ContactPerson(
    // generic fields
    concatId: String,
    countryCode: String,
    customerType: String,
    dateCreated: Option[Timestamp],
    dateUpdated: Option[Timestamp],
    isActive: Boolean,
    isGoldenRecord: Boolean,
    ohubId: Option[String],
    sourceEntityId: String,
    sourceName: String,
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp,
    operatorConcatId: String,
    operatorOhubId: Option[String],
    // specific fields
    oldIntegrationId: Option[String],
    firstName: Option[String],
    lastName: Option[String],
    title: Option[String],
    gender: Option[String],
    jobTitle: Option[String],
    language: Option[String],
    birthDate: Option[Timestamp],
    street: Option[String],
    houseNumber: Option[String],
    houseNumberExtension: Option[String],
    city: Option[String],
    zipCode: Option[String],
    state: Option[String],
    countryName: Option[String],
    isPreferredContact: Option[Boolean],
    isKeyDecisionMaker: Option[Boolean],
    standardCommunicationChannel: Option[String],
    emailAddress: Option[String],
    phoneNumber: Option[String],
    mobileNumber: Option[String],
    faxNumber: Option[String],
    hasGeneralOptOut: Option[Boolean],
    hasConfirmedRegistration: Option[Boolean],
    confirmedRegistrationDate: Option[Timestamp],
    hasEmailOptIn: Option[Boolean],
    emailOptInDate: Option[Timestamp],
    hasEmailDoubleOptIn: Option[Boolean],
    emailDoubleOptInDate: Option[Timestamp],
    hasEmailOptOut: Option[Boolean],
    hasDirectMailOptIn: Option[Boolean],
    hasDirectMailOptOut: Option[Boolean],
    hasTeleMarketingOptIn: Option[Boolean],
    hasTeleMarketingOptOut: Option[Boolean],
    hasMobileOptIn: Option[Boolean],
    mobileOptInDate: Option[Timestamp],
    hasMobileDoubleOptIn: Option[Boolean],
    mobileDoubleOptInDate: Option[Timestamp],
    hasMobileOptOut: Option[Boolean],
    hasFaxOptIn: Option[Boolean],
    hasFaxOptOut: Option[Boolean],
    webUpdaterId: Option[String],
    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity
