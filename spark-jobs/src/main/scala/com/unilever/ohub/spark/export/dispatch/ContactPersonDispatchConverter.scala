package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.export.dispatch.model.DispatchContactPerson
import com.unilever.ohub.spark.export._

object ContactPersonDispatchConverter extends Converter[ContactPerson, DispatchContactPerson] with TypeConversionFunctions with DispatchTransformationFunctions {

  override def convert(implicit cp: ContactPerson, explain: Boolean = false): DispatchContactPerson = {
    DispatchContactPerson(
      COUNTRY_CODE = getValue("countryCode"),
      CP_ORIG_INTEGRATION_ID = getValue("concatId"),
      CP_LNKD_INTEGRATION_ID = getValue("ohubId"),
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", Some(BooleanToYNConverter)),
      SOURCE = getValue("sourceName"),
      SOURCE_ID = getValue("sourceEntityId"),
      DELETE_FLAG = getValue("isActive", Some(InvertedBooleanToYNConverter)),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      GENDER = getValue("gender", Some(CleanString)),
      ROLE = getValue("jobTitle"),
      TITLE = getValue("title"),
      FIRST_NAME = getValue("firstName", Some(CleanString)),
      LAST_NAME = getValue("lastName", Some(CleanString)),
      STREET = getValue("street", Some(CleanString)),
      HOUSE_NUMBER = getValue("houseNumber", Some(CleanString)),
      HOUSE_NUMBER_ADD = getValue("houseNumberExtension", Some(CleanString)),
      ZIP_CODE = getValue("zipCode", Some(CleanString)),
      CITY = getValue("city", Some(CleanString)),
      COUNTRY = getValue("countryName"),
      DM_OPT_OUT = getValue("hasDirectMailOptOut", Some(BooleanToYNUCoverter)),
      EMAIL_ADDRESS = getValue("emailAddress", Some(new ClearInvalidEmail(cp.isEmailAddressValid))),
      EMAIL_OPT_OUT = getValue("hasEmailOptOut", Some(BooleanToYNUCoverter)),
      FIXED_OPT_OUT = getValue("hasTeleMarketingOptOut", Some(BooleanToYNUCoverter)),
      FIXED_PHONE_NUMBER = getValue("phoneNumber"),
      MOBILE_OPT_OUT = getValue("hasMobileOptOut", Some(BooleanToYNUCoverter)),
      MOBILE_PHONE_NUMBER = getValue("mobileNumber"),
      LANGUAGE = getValue("language"),
      PREFERRED = getValue("isPreferredContact", Some(BooleanToYNUCoverter)),
      KEY_DECISION_MAKER = getValue("isKeyDecisionMaker", Some(BooleanToYNUCoverter)),
      FAX_OPT_OUT = getValue("hasFaxOptOut", Some(BooleanToYNUCoverter)),
      FAX_NUMBER = getValue("faxNumber"),
      DATE_OF_BIRTH = getValue("birthDate"),
      SCM = getValue("standardCommunicationChannel"),
      STATE = getValue("state"),
      OPT_IN = getValue("hasEmailOptIn", Some(BooleanToYNUCoverter)),
      OPT_IN_DATE = getValue("emailOptInDate"),
      CONFIRMED_OPT_IN = getValue("hasEmailDoubleOptIn", Some(BooleanToYNUCoverter)),
      CONFIRMED_OPT_IN_DATE = getValue("emailDoubleOptInDate"),
      OPR_ORIG_INTEGRATION_ID = getValue("operatorConcatId"),
      MOB_OPT_IN = getValue("hasMobileOptIn", Some(BooleanToYNUCoverter)),
      MOB_OPT_IN_DATE = getValue("mobileOptInDate"),
      MOB_CONFIRMED_OPT_IN = getValue("hasMobileDoubleOptIn", Some(BooleanToYNUCoverter)),
      MOB_CONFIRMED_OPT_IN_DATE = getValue("mobileDoubleOptInDate")
    )
  }
}
