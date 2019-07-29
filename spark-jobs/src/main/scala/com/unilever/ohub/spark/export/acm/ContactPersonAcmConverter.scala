package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.export.acm.model.AcmContactPerson
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object ContactPersonAcmConverter extends Converter[ContactPerson, AcmContactPerson] with TransformationFunctions with AcmTransformationFunctions {

  override def convert(cp: ContactPerson): AcmContactPerson = {
    AcmContactPerson(
      CP_ORIG_INTEGRATION_ID = cp.ohubId,
      CP_LNKD_INTEGRATION_ID = new ConcatPersonOldOhubConverter(DomainDataProvider().sourceIds).convert(cp.concatId),
      OPR_ORIG_INTEGRATION_ID = cp.operatorOhubId,
      GOLDEN_RECORD_FLAG = booleanToYNConverter(cp.isGoldenRecord),
      EMAIL_OPTOUT = cp.hasEmailOptOut.booleanToYNU,
      PHONE_OPTOUT = cp.hasTeleMarketingOptOut.booleanToYNU,
      FAX_OPTOUT = cp.hasFaxOptOut.booleanToYNU,
      MOBILE_OPTOUT = cp.hasMobileOptOut.booleanToYNU,
      DM_OPTOUT = cp.hasDirectMailOptOut.booleanToYNU,
      LAST_NAME = cleanString(cp.lastName),
      FIRST_NAME = cleanString(cp.firstName),
      TITLE = cp.title,
      GENDER = cleanString(cp.gender) match {
        case "M" ⇒ "1"
        case "F" ⇒ "2"
        case _   ⇒ "0"
      },
      LANGUAGE = cp.language,
      EMAIL_ADDRESS = clearField(cp.emailAddress, cp.isEmailAddressValid),
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
      PREFERRED = cp.isPreferredContact.booleanToYNU,
      ROLE = cp.jobTitle,
      COUNTRY_CODE = cp.countryCode,
      SCM = cp.standardCommunicationChannel,
      DELETE_FLAG = booleanTo10Converter(!cp.isActive),
      KEY_DECISION_MAKER = cp.isKeyDecisionMaker.booleanToYN,
      OPT_IN = cp.hasEmailOptIn.booleanToYNU,
      OPT_IN_DATE = cp.emailOptInDate,
      CONFIRMED_OPT_IN = cp.hasEmailDoubleOptIn.booleanToYNU,
      CONFIRMED_OPT_IN_DATE = cp.emailDoubleOptInDate,
      MOB_OPT_IN = cp.hasMobileOptIn.booleanToYNU,
      MOB_OPT_IN_DATE = cp.mobileOptInDate,
      MOB_CONFIRMED_OPT_IN = cp.hasMobileDoubleOptIn.booleanToYNU,
      MOB_CONFIRMED_OPT_IN_DATE = cp.mobileDoubleOptInDate,
      ORG_FIRST_NAME = cleanString(cp.firstName),
      ORG_LAST_NAME = cleanString(cp.lastName),
      ORG_EMAIL_ADDRESS = clearField(cp.emailAddress, cp.isEmailAddressValid),
      ORG_FIXED_PHONE_NUMBER = cp.phoneNumber,
      ORG_MOBILE_PHONE_NUMBER = cp.mobileNumber,
      ORG_FAX_NUMBER = cp.faxNumber,
      HAS_REGISTRATION = cp.hasRegistration.booleanToYNU,
      REGISTRATION_DATE = cp.registrationDate,
      HAS_CONFIRMED_REGISTRATION = cp.hasConfirmedRegistration.booleanToYNU,
      CONFIRMED_REGISTRATION_DATE = cp.confirmedRegistrationDate,
      CONTACTPERSON_ID = cp.concatId
    )
  }
}
