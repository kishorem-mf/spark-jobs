package com.unilever.ohub.spark.ingest.file_interface

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.ingest.{ ContactPersonEmptyParquetWriter, DomainTransformer }
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.generic.StringFunctions._
import org.apache.spark.sql.Row

object ContactPersonConverter extends FileDomainGateKeeper[ContactPerson] with ContactPersonEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ ContactPerson = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    // format: OFF

    val countryCode       = mandatoryValue("COUNTRY_CODE", "countryCode")(row)
    val concatId          = createConcatId("COUNTRY_CODE", "SOURCE", "REF_CONTACT_PERSON_ID")
    val operatorConcatId  = createConcatId("COUNTRY_CODE", "SOURCE", "REF_OPERATOR_ID")
    val name              = concatValues("FIRST_NAME", "LAST_NAME")
    val ohubCreated       = currentTimestamp()

    // fieldName                        mandatory   sourceFieldName                   targetFieldName                 transformationFunction (unsafe)
    ContactPerson(
      concatId                      = concatId                                                                                                            ,
      countryCode                   = countryCode                                                                                                         ,
      customerType                  = ContactPerson.customerType                                                                                          ,
      ohubCreated                   = ohubCreated                                                                                                         ,
      ohubUpdated                   = ohubCreated                                                                                                         ,
      ohubId                        = Option.empty, // set in ContactPersonMatchingJoiner, ContactPersonExactMatcher, ContactPersonIntegratedExactMatch
      isGoldenRecord                = false                                                                                                               ,
      sourceEntityId                =   mandatory(  "REF_CONTACT_PERSON_ID",          "sourceEntityId"                                                   ),
      sourceName                    =   mandatory(  "SOURCE",                         "sourceName"                                                       ),
      isActive                      =   mandatory(	"STATUS",                         "isActive",                     parseBoolUnsafe                    ),
      dateCreated                   =   optional(   "DATE_CREATED",                   "dateCreated",                  parseDateTimeStampUnsafe           ),
      dateUpdated                   =   optional(   "DATE_MODIFIED",                  "dateUpdated",                  parseDateTimeStampUnsafe           ),
      name                          = name                                                                                                                ,
      operatorConcatId              = operatorConcatId                                                                                                    ,
      operatorOhubId                = Option.empty,  // set in ContactPersonMatchingJoiner
      oldIntegrationId              =   optional(   "CP_INTEGRATION_ID",              "oldIntegrationId"                                                 ),
      firstName                     =   optional(   "FIRST_NAME",                     "firstName"                                                        ),
      lastName                      =   optional(   "LAST_NAME",                      "lastName"                                                         ),
      title                         =   optional(   "TITLE",                          "title"                                                            ),
      gender                        =   optional(   "GENDER",                         "gender",                       checkEnum(ContactPerson.genderEnum) ),
      jobTitle                      =   optional(   "FUNCTION",                       "jobTitle"                                                         ),
      language                      =   optional(   "LANGUAGE_KEY",                   "language"                                                         ),
      birthDate                     =   optional(   "BIRTH_DATE",                     "birthDate",                    parseDateTimeStampUnsafe           ),
      street                        =   optional(   "STREET",                         "street"                                                           ),
      houseNumber                   =   optional(   "HOUSENUMBER",                    "houseNumber"                                                      ),
      houseNumberExtension          =   optional(   "HOUSENUMBER_EXT",                "houseNumberExtension"                                             ),
      city                          =   optional(   "CITY",                           "city"                                                             ),
      zipCode                       =   optional(   "ZIP_CODE",                       "zipCode"                                                          ),
      state                         =   optional(   "STATE",                          "state"                                                            ),
      countryName                   =   countryName(countryCode)                                                                                          ,
      isPreferredContact            =   optional(   "PREFERRED_CONTACT",              "isPreferredContact",           parseBoolUnsafe                    ),
      isKeyDecisionMaker            =   optional(   "KEY_DECISION_MAKER",             "isKeyDecisionMaker",           parseBoolUnsafe                    ),
      standardCommunicationChannel  =   optional(   "SCM",                            "standardCommunicationChannel"                                     ),
      emailAddress                  =   optional(   "EMAIL_ADDRESS",                  "emailAddress",                 checkEmailValidity                 ),
      phoneNumber                   =   optional(   "PHONE_NUMBER",                   "phoneNumber",                  cleanPhone(countryCode)            ),
      mobileNumber                  =   optional(   "MOBILE_PHONE_NUMBER",            "mobileNumber",                 cleanPhone(countryCode)            ),
      faxNumber                     =   optional(   "FAX_NUMBER",                     "faxNumber",                    cleanPhone(countryCode)            ),
      hasGeneralOptOut              =   optional(   "OPT_OUT",                        "hasGeneralOptOut",             parseBoolUnsafe                    ),
      hasConfirmedRegistration      =   optional(   "REGISTRATION_CONFIRMED",         "hasConfirmedRegistration",     parseBoolUnsafe                    ),
      confirmedRegistrationDate     =   optional(   "REGISTRATION_CONFIRMED_DATE",    "confirmedRegistrationDate",    parseDateTimeStampUnsafe           ),
      hasEmailOptIn                 =   optional(   "EM_OPT_IN",                      "hasEmailOptIn",                parseBoolUnsafe                    ),
      emailOptInDate                =   optional(   "EM_OPT_IN_DATE",                 "emailOptInDate",               parseDateTimeStampUnsafe           ),
      hasEmailDoubleOptIn           =   optional(   "EM_OPT_IN_CONFIRMED",            "hasEmailDoubleOptIn",          parseBoolUnsafe                    ),
      emailDoubleOptInDate          =   optional(   "EM_OPT_IN_CONFIRMED_DATE",       "emailDoubleOptInDate",         parseDateTimeStampUnsafe           ),
      hasEmailOptOut                =   optional(   "EM_OPT_OUT",                     "hasEmailOptOut",               parseBoolUnsafe                    ),
      hasDirectMailOptIn            =   optional(   "DM_OPT_IN",                      "hasDirectMailOptIn",           parseBoolUnsafe                    ),
      hasDirectMailOptOut           =   optional(   "DM_OPT_OUT",                     "hasDirectMailOptOut",          parseBoolUnsafe                    ),
      hasTeleMarketingOptIn         =   optional(   "TM_OPT_IN",                      "hasTeleMarketingOptIn",        parseBoolUnsafe                    ),
      hasTeleMarketingOptOut        =   optional(   "TM_OPT_OUT",                     "hasTeleMarketingOptOut",       parseBoolUnsafe                    ),
      hasMobileOptIn                =   optional(   "MOB_OPT_IN",                     "hasMobileOptIn",               parseBoolUnsafe                    ),
      mobileOptInDate               =   optional(   "MOB_OPT_IN_DATE",                "mobileOptInDate",              parseDateTimeStampUnsafe           ),
      hasMobileDoubleOptIn          =   optional(   "MOB_OPT_IN_CONFIRMED",           "hasMobileDoubleOptIn",         parseBoolUnsafe                    ),
      mobileDoubleOptInDate         =   optional(   "MOB_OPT_IN_CONFIRMED_DATE",      "mobileDoubleOptInDate",        parseDateTimeStampUnsafe           ),
      hasMobileOptOut               =   optional(   "MOB_OPT_OUT",                    "hasMobileOptOut",              parseBoolUnsafe                    ),
      hasFaxOptIn                   =   optional(   "FAX_OPT_IN",                     "hasFaxOptIn",                  parseBoolUnsafe                    ),
      hasFaxOptOut                  =   optional(   "FAX_OPT_OUT",                    "hasFaxOptOut",                 parseBoolUnsafe                    ),
      webUpdaterId                  = Option.empty,
      additionalFields              = additionalFields,
      ingestionErrors               = errors
    )
    // format: ON
  }
}
