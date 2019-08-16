package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.acm.model.AcmContactPerson

object ContactPersonAcmConverter extends Converter[ContactPerson, AcmContactPerson] with AcmTransformationFunctions {

  override def convert(cp: ContactPerson): AcmContactPerson = {
    implicit val input: ContactPerson = cp
    implicit val explain = false

    AcmContactPerson(
      CP_ORIG_INTEGRATION_ID = getValue("ohubId"),
      CP_LNKD_INTEGRATION_ID = getValue("concatId", Some(new ConcatPersonOldOhubConverter(DomainDataProvider().sourceIds))),
      OPR_ORIG_INTEGRATION_ID = getValue("operatorOhubId"),
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", Some(BooleanToYNConverter)),
      EMAIL_OPTOUT = getValue("hasEmailOptOut", Some(BooleanToYNUCoverter)),
      PHONE_OPTOUT = getValue("hasTeleMarketingOptOut", Some(BooleanToYNUCoverter)),
      FAX_OPTOUT = getValue("hasFaxOptOut", Some(BooleanToYNUCoverter)),
      MOBILE_OPTOUT = getValue("hasMobileOptOut", Some(BooleanToYNUCoverter)),
      DM_OPTOUT = getValue("hasDirectMailOptOut", Some(BooleanToYNUCoverter)),
      LAST_NAME = getValue("lastName", Some(CleanString)),
      FIRST_NAME = getValue("firstName", Some(CleanString)),
      TITLE = getValue("title"),
      GENDER = getValue("gender", Some(GenderToNumeric)),
      LANGUAGE = getValue("language"),
      EMAIL_ADDRESS = getValue("emailAddress", Some(new ClearInvalidEmail(cp.isEmailAddressValid))),
      MOBILE_PHONE_NUMBER = getValue("mobileNumber"),
      PHONE_NUMBER = getValue("phoneNumber"),
      FAX_NUMBER = getValue("faxNumber"),
      STREET = getValue("street", Some(CleanString)),
      HOUSENUMBER = getValue("houseNumber", Some(CleanString)),
      ZIPCODE = getValue("zipCode", Some(CleanString)),
      CITY = getValue("city", Some(CleanString)),
      COUNTRY = getValue("countryName"),
      DATE_CREATED = getValue("dateCreated"),
      DATE_UPDATED = getValue("dateUpdated"),
      DATE_OF_BIRTH = getValue("birthDate"),
      PREFERRED = getValue("isPreferredContact", Some(BooleanToYNUCoverter)),
      ROLE = getValue("jobTitle"),
      COUNTRY_CODE = getValue("countryCode"),
      SCM = getValue("standardCommunicationChannel"),
      DELETE_FLAG = getValue("isActive", Some(InvertedBooleanTo10Converter)),
      KEY_DECISION_MAKER = getValue("isKeyDecisionMaker", Some(BooleanToYNConverter)),
      OPT_IN = getValue("hasEmailOptIn", Some(BooleanToYNUCoverter)),
      OPT_IN_DATE = getValue("emailOptInDate"),
      CONFIRMED_OPT_IN = getValue("hasEmailDoubleOptIn", Some(BooleanToYNUCoverter)),
      CONFIRMED_OPT_IN_DATE = getValue("emailDoubleOptInDate"),
      MOB_OPT_IN = getValue("hasMobileOptIn", Some(BooleanToYNUCoverter)),
      MOB_OPT_IN_DATE = getValue("mobileOptInDate"),
      MOB_CONFIRMED_OPT_IN = getValue("hasMobileDoubleOptIn", Some(BooleanToYNUCoverter)),
      MOB_CONFIRMED_OPT_IN_DATE = getValue("mobileDoubleOptInDate"),
      ORG_FIRST_NAME = getValue("firstName", Some(CleanString)),
      ORG_LAST_NAME = getValue("lastName", Some(CleanString)),
      ORG_EMAIL_ADDRESS = clearField(cp.emailAddress, cp.isEmailAddressValid),
      ORG_FIXED_PHONE_NUMBER = getValue("phoneNumber"),
      ORG_MOBILE_PHONE_NUMBER = getValue("mobileNumber"),
      ORG_FAX_NUMBER = getValue("faxNumber"),
      HAS_REGISTRATION = getValue("hasRegistration", Some(BooleanToYNUCoverter)),
      REGISTRATION_DATE = getValue("registrationDate"),
      HAS_CONFIRMED_REGISTRATION = getValue("hasConfirmedRegistration", Some(BooleanToYNUCoverter)),
      CONFIRMED_REGISTRATION_DATE = getValue("confirmedRegistrationDate")
      //CONTACTPERSON_ID = cp.concatId
    )
  }
}
