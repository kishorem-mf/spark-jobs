package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.ingest.CustomParsers.{parseDateUnsafe, _}
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
        webUpdaterId = optional("webUpdaterId"),
        socialNetworkName = optional("socialNetworkName"),
        socialNetworkId = optional("socialNetworkId"),
        isEmailAddressValid = Some(true),
        isMobileNumberValid = Some(true),
        //crm fields
        crmId = optional("crmId"),
        otherJobTitle = optional("otherJobTitle"),
        optInSourceName = optional("optInSourceName"),
        subscriptionsList = optional("subscriptionsList"),
        executeRightToBeForgotten = optional("executeRightToBeForgotten",toBoolean),
        hasTelephoneSuppressed = optional("hasTelephoneSuppressed",toBoolean),
        emailOptOutDate = optional("emailOptOutDate",parseDateTimeUnsafe()),
        startWorkDate = optional("startWorkDate",parseDateTimeUnsafe()),
        endWorkDate = optional("endWorkDate",parseDateTimeUnsafe()),
        doNotCall = optional("doNotCall",toBoolean),
        emailOptInStatus = optional("emailOptInStatus"),
        emailConsentDate = optional("emailConsentDate",parseDateTimeUnsafe()),
        mobileOptOutDate = optional("mobileOptOutDate",parseDateTimeUnsafe()),
        mobileOptInStatus = optional("mobileOptInStatus"),
        mobileConsentDate = optional("mobileConsentDate",parseDateTimeUnsafe()),
        teleMarketingOptInDate = optional("teleMarketingOptInDate",parseDateTimeUnsafe()),
        hasTeleMarketingDoubleOptIn = optional("hasTeleMarketingDoubleOptIn",toBoolean),
        teleMarketingDoubleOptInDate = optional("teleMarketingDoubleOptInDate",parseDateTimeUnsafe()),
        teleMarketingOptOutDate = optional("teleMarketingOptOutDate",parseDateTimeUnsafe()),
        teleMarketingOptInStatus = optional("teleMarketingOptInStatus"),
        teleMarketingConsentDate = optional("teleMarketingConsentDate",parseDateTimeUnsafe()),
        hasCalculatedEmailConsent = optional("hasCalculatedEmailConsent",toBoolean),
        hasCalculatedMobileConsent = optional("hasCalculatedMobileConsent",toBoolean),
        hasCalculatedTeleMarketingConsent = optional("hasCalculatedTeleMarketingConsent",toBoolean),
        additionalFields = additionalFields,
        ingestionErrors = errors
      )
  }
}
