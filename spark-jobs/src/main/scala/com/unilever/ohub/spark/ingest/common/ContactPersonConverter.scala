package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ContactPersonEmptyParquetWriter, DomainTransformer}
import org.apache.spark.sql.Row

object ContactPersonConverter extends CommonDomainGateKeeper[ContactPerson] with ContactPersonEmptyParquetWriter {

  // scalastyle:off method.length
  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ ContactPerson = { transformer ⇒
    row ⇒
      import transformer._
      implicit val source: Row = row

      val ohubCreated = new Timestamp(System.currentTimeMillis())

      ContactPerson(
        id = mandatory("id"),
        creationTimestamp = mandatory("creationTimestamp", toTimestamp),
        concatId = mandatory("concatId"),
        countryCode = mandatory("countryCode"),
        customerType = ContactPerson.customerType,
        ohubCreated = ohubCreated,
        ohubUpdated = ohubCreated,
        ohubId = Option.empty, // set in ContactPersonMatchingJoiner, ContactPersonExactMatcher, ContactPersonIntegratedExactMatch
        isGoldenRecord = false,
        sourceEntityId = mandatory("sourceEntityId"),
        sourceName = mandatory("sourceName"),
        isActive = mandatory("isActive", toBoolean),
        dateCreated = optional("dateCreated", parseDateTimeUnsafe()),
        dateUpdated = optional("dateUpdated", parseDateTimeUnsafe()),
        operatorConcatId = optional("operatorConcatId"),
        operatorOhubId = Option.empty, // set in ContactPersonReferencing
        oldIntegrationId = optional("oldIntegrationId"),
        firstName = optional("firstName"),
        lastName = optional("lastName"),
        title = optional("title"),
        gender = optional("gender"),
        jobTitle = optional("jobTitle"),
        language = optional("language"),
        birthDate = optional("birthDate", parseDateUnsafe()),
        street = optional("street"),
        houseNumber = optional("houseNumber"),
        houseNumberExtension = optional("houseNumberExtension"),
        city = optional("city"),
        zipCode = optional("zipCode"),
        state = optional("state"),
        countryName = optional("countryName"),
        isPreferredContact = optional("isPreferredContact", toBoolean),
        isKeyDecisionMaker = optional("isKeyDecisionMaker", toBoolean),
        standardCommunicationChannel = optional("standardCommunicationChannel"),
        emailAddress = optional("emailAddress"),
        phoneNumber = optional("phoneNumber"),
        mobileNumber = optional("mobileNumber"),
        faxNumber = optional("faxNumber"),
        hasGeneralOptOut = optional("hasGeneralOptOut", toBoolean),
        hasRegistration = optional("hasRegistration", toBoolean),
        registrationDate = optional("registrationDate", parseDateTimeUnsafe()),
        hasConfirmedRegistration = optional("hasConfirmedRegistration", toBoolean),
        confirmedRegistrationDate = optional("confirmedRegistrationDate", parseDateTimeUnsafe()),
        hasEmailOptIn = optional("hasEmailOptIn", toBoolean),
        emailOptInDate = optional("emailOptInDate", parseDateTimeUnsafe()),
        hasEmailDoubleOptIn = optional("hasEmailDoubleOptIn", toBoolean),
        emailDoubleOptInDate = optional("emailDoubleOptInDate", parseDateTimeUnsafe()),
        hasEmailOptOut = optional("hasEmailOptOut", toBoolean),
        hasDirectMailOptIn = optional("hasDirectMailOptIn", toBoolean),
        hasDirectMailOptOut = optional("hasDirectMailOptOut", toBoolean),
        hasTeleMarketingOptIn = optional("hasTeleMarketingOptIn", toBoolean),
        hasTeleMarketingOptOut = optional("hasTeleMarketingOptOut", toBoolean),
        hasMobileOptIn = optional("hasMobileOptIn", toBoolean),
        mobileOptInDate = optional("mobileOptInDate", parseDateTimeUnsafe()),
        hasMobileDoubleOptIn = optional("hasMobileDoubleOptIn", toBoolean),
        mobileDoubleOptInDate = optional("mobileDoubleOptInDate", parseDateTimeUnsafe()),
        hasMobileOptOut = optional("hasMobileOptOut", toBoolean),
        hasFaxOptIn = optional("hasFaxOptIn", toBoolean),
        hasFaxOptOut = optional("hasFaxOptOut", toBoolean),
        webUpdaterId = Option.empty, // TODO what to do with this one?
        isEmailAddressValid = Some(true),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
