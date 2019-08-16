package com.unilever.ohub.spark.export.acm

import com.unilever.ohub.spark.DomainDataProvider
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.export._
import com.unilever.ohub.spark.export.acm.model.AcmContactPerson

object ContactPersonAcmConverter extends Converter[ContactPerson, AcmContactPerson] with AcmTransformationFunctions {

  override def convert(implicit cp: ContactPerson, explain: Boolean = false): AcmContactPerson = {
    AcmContactPerson(
      CP_ORIG_INTEGRATION_ID = getValue("ohubId"),
      CP_LNKD_INTEGRATION_ID = getValue("concatId", new ConcatPersonOldOhubConverter(DomainDataProvider().sourceIds)),
      OPR_ORIG_INTEGRATION_ID = getValue("operatorOhubId"),
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", BooleanToYNConverter),
      EMAIL_OPTOUT = getValue("hasEmailOptOut", BooleanToYNUCoverter),
      PHONE_OPTOUT = getValue("hasTeleMarketingOptOut", BooleanToYNUCoverter),
      FAX_OPTOUT = getValue("hasFaxOptOut", BooleanToYNUCoverter),
      MOBILE_OPTOUT = getValue("hasMobileOptOut", BooleanToYNUCoverter),
      DM_OPTOUT = getValue("hasDirectMailOptOut", BooleanToYNUCoverter),
      LAST_NAME = getValue("lastName", CleanString),
      FIRST_NAME = getValue("firstName", CleanString),
      TITLE = getValue("title"),
      GENDER = getValue("gender", GenderToNumeric),
      LANGUAGE = getValue("language"),
      EMAIL_ADDRESS = getValue("emailAddress", new ClearInvalidEmail(cp.isEmailAddressValid)),
      MOBILE_PHONE_NUMBER = getValue("mobileNumber"),
      PHONE_NUMBER = getValue("phoneNumber"),
      FAX_NUMBER = getValue("faxNumber"),
      STREET = getValue("street", CleanString),
      HOUSENUMBER = getValue("houseNumber", CleanString),
      ZIPCODE = getValue("zipCode", CleanString),
      CITY = getValue("city", CleanString),
      COUNTRY = getValue("countryName"),
      DATE_CREATED = getValue("dateCreated"),
      DATE_UPDATED = getValue("dateUpdated"),
      DATE_OF_BIRTH = getValue("birthDate"),
      PREFERRED = getValue("isPreferredContact", BooleanToYNUCoverter),
      ROLE = getValue("jobTitle"),
      COUNTRY_CODE = getValue("countryCode"),
      SCM = getValue("standardCommunicationChannel"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanTo10Converter),
      KEY_DECISION_MAKER = getValue("isKeyDecisionMaker", BooleanToYNConverter),
      OPT_IN = getValue("hasEmailOptIn", BooleanToYNUCoverter),
      OPT_IN_DATE = getValue("emailOptInDate"),
      CONFIRMED_OPT_IN = getValue("hasEmailDoubleOptIn", BooleanToYNUCoverter),
      CONFIRMED_OPT_IN_DATE = getValue("emailDoubleOptInDate"),
      MOB_OPT_IN = getValue("hasMobileOptIn", BooleanToYNUCoverter),
      MOB_OPT_IN_DATE = getValue("mobileOptInDate"),
      MOB_CONFIRMED_OPT_IN = getValue("hasMobileDoubleOptIn", BooleanToYNUCoverter),
      MOB_CONFIRMED_OPT_IN_DATE = getValue("mobileDoubleOptInDate"),
      ORG_FIRST_NAME = getValue("firstName", CleanString),
      ORG_LAST_NAME = getValue("lastName", CleanString),
      ORG_EMAIL_ADDRESS = getValue("emailAddress", new ClearInvalidEmail(cp.isEmailAddressValid)),
      ORG_FIXED_PHONE_NUMBER = getValue("phoneNumber"),
      ORG_MOBILE_PHONE_NUMBER = getValue("mobileNumber"),
      ORG_FAX_NUMBER = getValue("faxNumber"),
      HAS_REGISTRATION = getValue("hasRegistration", BooleanToYNUCoverter),
      REGISTRATION_DATE = getValue("registrationDate"),
      HAS_CONFIRMED_REGISTRATION = getValue("hasConfirmedRegistration", BooleanToYNUCoverter),
      CONFIRMED_REGISTRATION_DATE = getValue("confirmedRegistrationDate")
      //CONTACTPERSON_ID = cp.concatId
    )
  }
}
