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
      GOLDEN_RECORD_FLAG = getValue("isGoldenRecord", BooleanToYNConverter),
      SOURCE = getValue("sourceName"),
      SOURCE_ID = getValue("sourceEntityId"),
      DELETE_FLAG = getValue("isActive", InvertedBooleanToYNConverter),
      CREATED_AT = getValue("ohubCreated"),
      UPDATED_AT = getValue("ohubUpdated"),
      GENDER = getValue("gender", CleanString),
      ROLE = getValue("jobTitle"),
      TITLE = getValue("title"),
      FIRST_NAME = getValue("firstName", CleanString),
      LAST_NAME = getValue("lastName", CleanString),
      STREET = getValue("street", CleanString),
      HOUSE_NUMBER = getValue("houseNumber", CleanString),
      HOUSE_NUMBER_ADD = getValue("houseNumberExtension", CleanString),
      ZIP_CODE = getValue("zipCode", CleanString),
      CITY = getValue("city", CleanString),
      COUNTRY = getValue("countryName"),
      DM_OPT_OUT = getValue("hasDirectMailOptOut", BooleanToYNUConverter),
      EMAIL_ADDRESS = getValue("emailAddress", new ClearInvalidEmail(cp.isEmailAddressValid)),
      EMAIL_OPT_OUT = getValue("hasEmailOptOut", BooleanToYNUConverter),
      FIXED_OPT_OUT = getValue("hasTeleMarketingOptOut", BooleanToYNUConverter),
      FIXED_PHONE_NUMBER = getValue("phoneNumber"),
      MOBILE_OPT_OUT = getValue("hasMobileOptOut", BooleanToYNUConverter),
      MOBILE_PHONE_NUMBER = getValue("mobileNumber"),
      LANGUAGE = getValue("language"),
      PREFERRED = getValue("isPreferredContact", BooleanToYNUConverter),
      KEY_DECISION_MAKER = getValue("isKeyDecisionMaker", BooleanToYNUConverter),
      FAX_OPT_OUT = getValue("hasFaxOptOut", BooleanToYNUConverter),
      FAX_NUMBER = getValue("faxNumber"),
      DATE_OF_BIRTH = getValue("birthDate"),
      SCM = getValue("standardCommunicationChannel"),
      STATE = getValue("state"),
      OPT_IN = getValue("hasEmailOptIn", BooleanToYNUConverter),
      OPT_IN_DATE = getValue("emailOptInDate"),
      CONFIRMED_OPT_IN = getValue("hasEmailDoubleOptIn", BooleanToYNUConverter),
      CONFIRMED_OPT_IN_DATE = getValue("emailDoubleOptInDate"),
      OPR_ORIG_INTEGRATION_ID = getValue("operatorConcatId"),
      MOB_OPT_IN = getValue("hasMobileOptIn", BooleanToYNUConverter),
      MOB_OPT_IN_DATE = getValue("mobileOptInDate"),
      MOB_CONFIRMED_OPT_IN = getValue("hasMobileDoubleOptIn", BooleanToYNUConverter),
      MOB_CONFIRMED_OPT_IN_DATE = getValue("mobileDoubleOptInDate")
    )
  }
}
