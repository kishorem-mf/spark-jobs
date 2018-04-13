package com.unilever.ohub.spark.tsv2parquet.file_interface

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import com.unilever.ohub.spark.generic.StringFunctions._
import org.apache.spark.sql.Row

object ContactPersonConverter extends FileDomainGateKeeper[ContactPerson] {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ ContactPerson = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val countryCode: String = originalValue("COUNTRY_CODE")(row).get
    val concatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_CONTACT_PERSON_ID")
    val ohubCreated = currentTimestamp()

      // format: OFF

      // fieldName                        mandatory   sourceFieldName                   targetFieldName                 transformationFunction (unsafe)
      ContactPerson(
        concatId                      =   concatId                                                                                                          ,
        countryCode                   =   mandatory(  "COUNTRY_CODE",                   "countryCode"                                                      ),
        customerType                  = ContactPerson.customerType                                                                                          ,
        ohubCreated                   = ohubCreated                                                                                                         ,
        ohubUpdated                   = ohubCreated                                                                                                         ,
        ohubId                        = Option.empty                                                                                                        ,
        isGoldenRecord                = false                                                                                                               ,
        sourceEntityId                =   mandatory(  "REF_CONTACT_PERSON_ID",          "sourceEntityId"                                                   ),
        sourceName                    =   mandatory(  "SOURCE",                         "sourceName"                                                       ),
        isActive                      =   mandatory(	"STATUS",                         "isActive",                     parseBoolUnsafe                    ),
        dateCreated                   =   optional(   "DATE_CREATED",                   "dateCreated",                  parseDateTimeStampUnsafe           ),
        dateUpdated                   =   optional(   "DATE_MODIFIED",                  "dateUpdated",                  parseDateTimeStampUnsafe           ),
        name                          = "TODO"                                                                                                              , // TODO is name a field in domain entity? should it be optional? what's the value for contact person?
        oldIntegrationId              =   optional(   "CP_INTEGRATION_ID",              "oldIntegrationId"                                                 ),
        firstName                     =   optional(   "FIRST_NAME",                     "firstName"                                                        ),
        lastName                      =   optional(   "LAST_NAME",                      "lastName"                                                         ),
        title                         =   optional(   "TITLE",                          "title"                                                            ),
        gender                        =   optional(   "GENDER",                         "gender"                                                           ),
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
