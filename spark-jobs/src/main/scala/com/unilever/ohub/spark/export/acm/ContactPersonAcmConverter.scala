package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.export.acm.model.AcmContactPerson
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object ContactPersonAcmConverter extends Converter[ContactPerson, AcmContactPerson] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(cp: ContactPerson): AcmContactPerson = {
    AcmContactPerson(
      CP_ORIG_INTEGRATION_ID = new ConcatPersonOldOhubConverter(DomainDataProvider().sourceIds).convert(cp.concatId),
      CP_LNKD_INTEGRATION_ID = cp.ohubId,
      OPR_ORIG_INTEGRATION_ID = cp.operatorOhubId,
      GOLDEN_RECORD_FLAG = "Y",
      EMAIL_OPTOUT = cp.hasEmailOptOut,
      PHONE_OPTOUT = cp.hasTeleMarketingOptOut,
      FAX_OPTOUT = cp.hasFaxOptOut,
      MOBILE_OPTOUT = cp.hasMobileOptOut,
      DM_OPTOUT = cp.hasDirectMailOptOut,
      LAST_NAME = cp.lastName,
      FIRST_NAME = cleanString(cp.firstName),
      TITLE = cp.title,
      GENDER = cp.gender.map {
        case "M" ⇒ "1"
        case "F" ⇒ "2"
        case _   ⇒ "0"
      },
      LANGUAGE = cp.language,
      EMAIL_ADDRESS = cp.emailAddress,
      MOBILE_PHONE_NUMBER = cp.mobileNumber,
      PHONE_NUMBER = cp.phoneNumber,
      FAX_NUMBER = cp.faxNumber,
      STREET = cp.street.map(cleanString),
      HOUSENUMBER = cp.houseNumber.map(cleanString),
      ZIPCODE = cp.zipCode.map(cleanString),
      CITY = cp.city.map(cleanString),
      COUNTRY = cp.countryName,
      DATE_CREATED = cp.dateCreated,
      DATE_UPDATED = cp.dateUpdated,
      DATE_OF_BIRTH = cp.birthDate,
      PREFERRED = cp.isPreferredContact,
      ROLE = cp.jobTitle,
      COUNTRY_CODE = cp.countryCode,
      SCM = cp.standardCommunicationChannel,
      DELETE_FLAG = booleanTo10Converter(!cp.isActive),
      KEY_DECISION_MAKER = cp.isKeyDecisionMaker,
      OPT_IN = cp.hasEmailOptIn,
      OPT_IN_DATE = cp.emailOptInDate,
      CONFIRMED_OPT_IN = cp.hasConfirmedRegistration,
      CONFIRMED_OPT_IN_DATE = cp.confirmedRegistrationDate,
      MOB_OPT_IN = cp.hasMobileOptIn,
      MOB_OPT_IN_DATE = cp.mobileOptInDate,
      MOB_CONFIRMED_OPT_IN = cp.hasMobileDoubleOptIn,
      MOB_CONFIRMED_OPT_IN_DATE = cp.mobileDoubleOptInDate,
      ORG_FIRST_NAME = cp.firstName,
      ORG_LAST_NAME = cp.lastName,
      ORG_EMAIL_ADDRESS = cp.emailAddress,
      ORG_FIXED_PHONE_NUMBER = cp.phoneNumber,
      ORG_MOBILE_PHONE_NUMBER = cp.mobileNumber,
      ORG_FAX_NUMBER = cp.faxNumber,
      HAS_REGISTRATION = cp.hasRegistration,
      REGISTRATION_DATE = cp.registrationDate,
      HAS_CONFIRMED_REGISTRATION = cp.hasConfirmedRegistration,
      CONFIRMED_REGISTRATION_DATE = cp.confirmedRegistrationDate
    )
  }
}
