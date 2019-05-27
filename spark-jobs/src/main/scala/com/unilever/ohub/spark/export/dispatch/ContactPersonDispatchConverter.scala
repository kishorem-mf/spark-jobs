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
      GOLDEN_RECORD_FLAG = cp.isGoldenRecord,
      SOURCE = cp.sourceName,
      SOURCE_ID = cp.sourceEntityId,
      DELETE_FLAG = !cp.isActive,
      CREATED_AT = cp.ohubCreated,
      UPDATED_AT = cp.ohubUpdated,
      GENDER = cp.gender.map(cleanString),
      ROLE = cp.jobTitle,
      TITLE = cp.title,
      FIRST_NAME = cp.firstName.map(cleanString),
      LAST_NAME = cp.lastName.map(cleanString),
      STREET = cp.street.map(cleanString),
      HOUSE_NUMBER = cp.houseNumber,
      HOUSE_NUMBER_ADD = cp.houseNumberExtension.map(cleanString),
      ZIP_CODE = cp.zipCode.map(cleanString),
      CITY = cp.city.map(cleanString),
      COUNTRY = cp.countryName,
      DM_OPT_OUT = cp.hasDirectMailOptOut,
      EMAIL_ADDRESS = cp.emailAddress,
      EMAIL_OPT_OUT = cp.hasEmailOptOut,
      FIXED_OPT_OUT = cp.hasTeleMarketingOptOut,
      FIXED_PHONE_NUMBER = cp.phoneNumber,
      MOBILE_OPT_OUT = cp.hasMobileOptOut,
      MOBILE_PHONE_NUMBER = cp.mobileNumber,
      LANGUAGE = cp.language,
      PREFERRED = cp.isPreferredContact,
      KEY_DECISION_MAKER = cp.isKeyDecisionMaker,
      FAX_OPT_OUT = cp.hasFaxOptOut,
      FAX_NUMBER = cp.faxNumber,
      DATE_OF_BIRTH = cp.birthDate,
      SCM = cp.standardCommunicationChannel,
      STATE = cp.state,
      OPT_IN = cp.hasEmailOptIn,
      OPT_IN_DATE = cp.emailOptInDate,
      CONFIRMED_OPT_IN = cp.hasEmailDoubleOptIn,
      CONFIRMED_OPT_IN_DATE = cp.emailDoubleOptInDate,
      OPR_ORIG_INTEGRATION_ID = cp.operatorConcatId,
      MOB_OPT_IN = cp.hasMobileOptIn,
      MOB_OPT_IN_DATE = cp.mobileOptInDate,
      MOB_CONFIRMED_OPT_IN = cp.hasMobileDoubleOptIn,
      MOB_CONFIRMED_OPT_IN_DATE = cp.mobileDoubleOptInDate
    )
  }
}
