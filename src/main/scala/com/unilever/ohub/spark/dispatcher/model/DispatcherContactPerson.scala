package com.unilever.ohub.spark.dispatcher
package model

import com.unilever.ohub.spark.domain.entity.ContactPerson

object DispatcherContactPerson {
  def fromContactPerson(cp: ContactPerson): DispatcherContactPerson = {
    val (concatId, countryCode, customerType, dateCreated, dateUpdated, isActive, isGoldenRecord, ohubId, name, sourceEntityId, sourceName, ohubCreated, ohubUpdated, operatorConcatId, operatorOhubId, oldIntegrationId, firstName, lastName, title, gender, jobTitle, language, birthDate, street, houseNumber, houseNumberExtension, city, zipCode, state, countryName, isPreferredContact, isKeyDecisionMaker, standardCommunicationChannel, emailAddress, phoneNumber, mobileNumber, faxNumber, hasGeneralOptOut, hasConfirmedRegistration, confirmedRegistrationDate, hasEmailOptIn, emailOptInDate, hasEmailDoubleOptIn, emailDoubleOptInDate, hasEmailOptOut, hasDirectMailOptIn, hasDirectMailOptOut, hasTeleMarketingOptIn, hasTeleMarketingOptOut, hasMobileOptIn, mobileOptInDate, hasMobileDoubleOptIn, mobileDoubleOptInDate, hasMobileOptOut, hasFaxOptIn, hasFaxOptOut, webUpdaterId, additionalFields, ingestionErrors) = ContactPerson.unapply(cp).get
    DispatcherContactPerson(
      DATE_OF_BIRTH = birthDate.mapWithDefaultPatternOpt,
      CITY = city,
      CP_ORIG_INTEGRATION_ID = concatId,
      COUNTRY_CODE = countryCode,
      COUNTRY = countryName,
      EMAIL_ADDRESS = emailAddress,
      CONFIRMED_OPT_IN_DATE = emailDoubleOptInDate.mapWithDefaultPatternOpt,
      OPT_IN_DATE = emailOptInDate.mapWithDefaultPatternOpt,
      FAX_NUMBER = faxNumber,
      GENDER = gender,
      DM_OPT_OUT = hasDirectMailOptOut,
      CONFIRMED_OPT_IN = hasEmailDoubleOptIn,
      OPT_IN = hasEmailOptIn,
      EMAIL_OPT_OUT = hasEmailOptOut,
      FAX_OPT_OUT = hasFaxOptOut,
      MOB_CONFIRMED_OPT_IN = hasMobileDoubleOptIn,
      MOB_OPT_IN = hasMobileOptIn,
      MOBILE_OPT_OUT = hasMobileOptOut,
      FIXED_OPT_OUT = hasTeleMarketingOptOut,
      HOUSE_NUMBER = houseNumber,
      HOUSE_NUMBER_ADD = houseNumberExtension,
      DELETE_FLAG = isActive.invert,
      GOLDEN_RECORD_FLAG = isGoldenRecord,
      KEY_DECISION_MAKER = isKeyDecisionMaker,
      PREFERRED = isPreferredContact,
      LANGUAGE = language,
      LAST_NAME = lastName,
      MOB_CONFIRMED_OPT_IN_DATE = mobileDoubleOptInDate.mapWithDefaultPatternOpt,
      MOBILE_PHONE_NUMBER = mobileNumber,
      MOB_OPT_IN_DATE = mobileOptInDate.mapWithDefaultPatternOpt,
      CREATED_AT = ohubCreated.mapWithDefaultPattern,
      CP_LNKD_INTEGRATION_ID = ohubId,
      UPDATED_AT = ohubUpdated.mapWithDefaultPattern,
      OPR_ORIG_INTEGRATION_ID = operatorConcatId,
      FIXED_PHONE_NUMBER = phoneNumber,
      SOURCE_ID = sourceEntityId,
      SOURCE = sourceName,
      SCM = standardCommunicationChannel,
      STATE = state,
      STREET = street,
      TITLE = title,
      ZIP_CODE = zipCode,
      MIDDLE_NAME = Option.empty,
      ROLE = jobTitle,
      ORG_FIRST_NAME = Option.empty,
      ORG_LAST_NAME = Option.empty,
      ORG_EMAIL_ADDRESS = Option.empty,
      ORG_FIXED_PHONE_NUMBER = Option.empty,
      ORG_MOBILE_PHONE_NUMBER = Option.empty,
      ORG_FAX_NUMBER = Option.empty,
      MOB_OPT_OUT_DATE = Option.empty
    )
  }
}

case class DispatcherContactPerson(
    DATE_OF_BIRTH: Option[String],
    CITY: Option[String],
    CP_ORIG_INTEGRATION_ID: String,
    COUNTRY_CODE: String,
    COUNTRY: Option[String],
    EMAIL_ADDRESS: Option[String],
    CONFIRMED_OPT_IN_DATE: Option[String],
    OPT_IN_DATE: Option[String],
    FAX_NUMBER: Option[String],
    GENDER: Option[String],
    DM_OPT_OUT: Option[Boolean],
    CONFIRMED_OPT_IN: Option[Boolean],
    OPT_IN: Option[Boolean],
    EMAIL_OPT_OUT: Option[Boolean],
    FAX_OPT_OUT: Option[Boolean],
    MOB_CONFIRMED_OPT_IN: Option[Boolean],
    MOB_OPT_IN: Option[Boolean],
    MOBILE_OPT_OUT: Option[Boolean],
    FIXED_OPT_OUT: Option[Boolean],
    HOUSE_NUMBER: Option[String],
    HOUSE_NUMBER_ADD: Option[String],
    DELETE_FLAG: Boolean,
    GOLDEN_RECORD_FLAG: Boolean,
    KEY_DECISION_MAKER: Option[Boolean],
    PREFERRED: Option[Boolean],
    LANGUAGE: Option[String],
    LAST_NAME: Option[String],
    MOB_CONFIRMED_OPT_IN_DATE: Option[String],
    MOBILE_PHONE_NUMBER: Option[String],
    MOB_OPT_IN_DATE: Option[String],
    CREATED_AT: String,
    CP_LNKD_INTEGRATION_ID: Option[String],
    UPDATED_AT: String,
    OPR_ORIG_INTEGRATION_ID: String,
    FIXED_PHONE_NUMBER: Option[String],
    SOURCE_ID: String,
    SOURCE: String,
    SCM: Option[String],
    STATE: Option[String],
    STREET: Option[String],
    TITLE: Option[String],
    ZIP_CODE: Option[String],
    MIDDLE_NAME: Option[String],
    ROLE: Option[String],
    ORG_FIRST_NAME: Option[String],
    ORG_LAST_NAME: Option[String],
    ORG_EMAIL_ADDRESS: Option[String],
    ORG_FIXED_PHONE_NUMBER: Option[String],
    ORG_MOBILE_PHONE_NUMBER: Option[String],
    ORG_FAX_NUMBER: Option[String],
    MOB_OPT_OUT_DATE: Option[String]
)
