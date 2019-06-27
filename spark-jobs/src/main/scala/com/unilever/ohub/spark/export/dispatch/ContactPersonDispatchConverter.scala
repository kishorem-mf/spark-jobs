package com.unilever.ohub.spark.export.dispatch

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.export.dispatch.model.DispatchContactPerson
import com.unilever.ohub.spark.export.{Converter, TransformationFunctions}

object ContactPersonDispatchConverter extends Converter[ContactPerson, DispatchContactPerson] with TransformationFunctions with DispatchTransformationFunctions {

  override def convert(cp: ContactPerson): DispatchContactPerson = {
    DispatchContactPerson(
      COUNTRY_CODE = cp.countryCode,
      CP_ORIG_INTEGRATION_ID = cp.concatId,
      CP_LNKD_INTEGRATION_ID = cp.ohubId,
      GOLDEN_RECORD_FLAG = booleanToYNConverter(cp.isGoldenRecord),
      SOURCE = cp.sourceName,
      SOURCE_ID = cp.sourceEntityId,
      DELETE_FLAG = booleanToYNConverter(!cp.isActive),
      CREATED_AT = cp.ohubCreated,
      UPDATED_AT = cp.ohubUpdated,
      GENDER = cp.gender.map(cleanString),
      ROLE = cp.jobTitle,
      TITLE = cp.title,
      FIRST_NAME = cp.firstName.map(cleanString),
      LAST_NAME = cp.lastName.map(cleanString),
      STREET = cp.street.map(cleanString),
      HOUSE_NUMBER = cp.houseNumber.map(cleanString),
      HOUSE_NUMBER_ADD = cp.houseNumberExtension.map(cleanString),
      ZIP_CODE = cp.zipCode.map(cleanString),
      CITY = cp.city.map(cleanString),
      COUNTRY = cp.countryName,
      DM_OPT_OUT = cp.hasDirectMailOptOut.booleanToYNU,
      EMAIL_ADDRESS = clearField(cp.emailAddress, cp.isEmailAddressValid),
      EMAIL_OPT_OUT = cp.hasEmailOptOut.booleanToYNU,
      FIXED_OPT_OUT = cp.hasTeleMarketingOptOut.booleanToYNU,
      FIXED_PHONE_NUMBER = cp.phoneNumber,
      MOBILE_OPT_OUT = cp.hasMobileOptOut.booleanToYNU,
      MOBILE_PHONE_NUMBER = cp.mobileNumber,
      LANGUAGE = cp.language,
      PREFERRED = cp.isPreferredContact.booleanToYNU,
      KEY_DECISION_MAKER = cp.isKeyDecisionMaker.booleanToYNU,
      FAX_OPT_OUT = cp.hasFaxOptOut.booleanToYNU,
      FAX_NUMBER = cp.faxNumber,
      DATE_OF_BIRTH = cp.birthDate,
      SCM = cp.standardCommunicationChannel,
      STATE = cp.state,
      OPT_IN = cp.hasEmailOptIn.booleanToYNU,
      OPT_IN_DATE = cp.emailOptInDate,
      CONFIRMED_OPT_IN = cp.hasEmailDoubleOptIn.booleanToYNU,
      CONFIRMED_OPT_IN_DATE = cp.emailDoubleOptInDate,
      OPR_ORIG_INTEGRATION_ID = cp.operatorConcatId,
      MOB_OPT_IN = cp.hasMobileOptIn.booleanToYNU,
      MOB_OPT_IN_DATE = cp.mobileOptInDate,
      MOB_CONFIRMED_OPT_IN = cp.hasMobileDoubleOptIn.booleanToYNU,
      MOB_CONFIRMED_OPT_IN_DATE = cp.mobileDoubleOptInDate
    )
  }
}
