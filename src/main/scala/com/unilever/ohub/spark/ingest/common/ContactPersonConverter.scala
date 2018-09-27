package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ ContactPersonEmptyParquetWriter, DomainTransformer }
import org.apache.spark.sql.Row

object ContactPersonConverter extends CommonDomainGateKeeper[ContactPerson] with ContactPersonEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ ContactPerson = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    // format: OFF

    val countryCode       = mandatoryValue("countryCode", "countryCode")(row)
    val concatId          = createConcatId("countryCode", "sourceName", "sourceEntityId")
    val operatorConcatId  = createConcatId("countryCode", "sourceName", "operatorRefId")
    val name              = concatValues("firstName", "lastName")
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
      sourceEntityId                =   mandatory(  "sourceEntityId",                 "sourceEntityId"                                                   ),
      sourceName                    =   mandatory(  "sourceName",                     "sourceName"                                                       ),
      isActive                      =   mandatory(	"isActive",                       "isActive",                     parseBoolUnsafe                    ),
      dateCreated                   =   optional(   "dateCreated",                    "dateCreated",                  parseDateTimeStampUnsafe           ),
      dateUpdated                   =   optional(   "dateUpdated",                    "dateUpdated",                  parseDateTimeStampUnsafe           ),
      name                          = name                                                                                                                ,
      operatorConcatId              = operatorConcatId                                                                                                    ,
      operatorOhubId                = Option.empty,  // set in ContactPersonMatchingJoiner
      oldIntegrationId              =   optional(   "oldIntegrationId",               "oldIntegrationId"                                                 ),
      firstName                     =   optional(   "firstName",                      "firstName"                                                        ),
      lastName                      =   optional(   "lastName",                       "lastName"                                                         ),
      title                         =   optional(   "title",                          "title"                                                            ),
      gender                        =   optional(   "gender",                         "gender",                       checkEnum(ContactPerson.genderEnum) ),
      jobTitle                      =   optional(   "jobTitle",                       "jobTitle"                                                         ),
      language                      =   optional(   "language",                       "language"                                                         ),
      birthDate                     =   optional(   "birthDate",                      "birthDate",                    parseDateTimeStampUnsafe           ),
      street                        =   optional(   "street",                         "street"                                                           ),
      houseNumber                   =   optional(   "houseNumber",                    "houseNumber"                                                      ),
      houseNumberExtension          =   optional(   "houseNumberExtension",           "houseNumberExtension"                                             ),
      city                          =   optional(   "city",                           "city"                                                             ),
      zipCode                       =   optional(   "zipCode",                        "zipCode"                                                          ),
      state                         =   optional(   "state",                          "state"                                                            ),
      countryName                   =   countryName(countryCode)                                                                                          ,
      isPreferredContact            =   optional(   "isPreferredContact",             "isPreferredContact",           parseBoolUnsafe                    ),
      isKeyDecisionMaker            =   optional(   "isKeyDecisionMaker",             "isKeyDecisionMaker",           parseBoolUnsafe                    ),
      standardCommunicationChannel  =   optional(   "standardCommunicationChannel",   "standardCommunicationChannel"                                     ),
      emailAddress                  =   optional(   "emailAddress",                   "emailAddress",                 checkEmailValidity                 ),
      phoneNumber                   =   optional(   "phoneNumber",                    "phoneNumber",                  cleanPhone(countryCode)            ),
      mobileNumber                  =   optional(   "mobileNumber",                   "mobileNumber",                 cleanPhone(countryCode)            ),
      faxNumber                     =   optional(   "faxNumber",                      "faxNumber",                    cleanPhone(countryCode)            ),
      hasGeneralOptOut              =   optional(   "hasGeneralOptOut",               "hasGeneralOptOut",             parseBoolUnsafe                    ),
      hasConfirmedRegistration      =   optional(   "hasConfirmedRegistration",       "hasConfirmedRegistration",     parseBoolUnsafe                    ),
      confirmedRegistrationDate     =   optional(   "confirmedRegistrationDate",      "confirmedRegistrationDate",    parseDateTimeStampUnsafe           ),
      hasEmailOptIn                 =   optional(   "hasEmailOptIn",                  "hasEmailOptIn",                parseBoolUnsafe                    ),
      emailOptInDate                =   optional(   "emailOptInDate",                 "emailOptInDate",               parseDateTimeStampUnsafe           ),
      hasEmailDoubleOptIn           =   optional(   "hasEmailDoubleOptIn",            "hasEmailDoubleOptIn",          parseBoolUnsafe                    ),
      emailDoubleOptInDate          =   optional(   "emailDoubleOptInDate",           "emailDoubleOptInDate",         parseDateTimeStampUnsafe           ),
      hasEmailOptOut                =   optional(   "hasEmailOptOut",                 "hasEmailOptOut",               parseBoolUnsafe                    ),
      hasDirectMailOptIn            =   optional(   "hasDirectMailOptIn",             "hasDirectMailOptIn",           parseBoolUnsafe                    ),
      hasDirectMailOptOut           =   optional(   "hasDirectMailOptOut",            "hasDirectMailOptOut",          parseBoolUnsafe                    ),
      hasTeleMarketingOptIn         =   optional(   "hasTeleMarketingOptIn",          "hasTeleMarketingOptIn",        parseBoolUnsafe                    ),
      hasTeleMarketingOptOut        =   optional(   "hasTeleMarketingOptOut",         "hasTeleMarketingOptOut",       parseBoolUnsafe                    ),
      hasMobileOptIn                =   optional(   "hasMobileOptIn",                 "hasMobileOptIn",               parseBoolUnsafe                    ),
      mobileOptInDate               =   optional(   "mobileOptInDate",                "mobileOptInDate",              parseDateTimeStampUnsafe           ),
      hasMobileDoubleOptIn          =   optional(   "hasMobileDoubleOptIn",           "hasMobileDoubleOptIn",         parseBoolUnsafe                    ),
      mobileDoubleOptInDate         =   optional(   "mobileDoubleOptInDate",          "mobileDoubleOptInDate",        parseDateTimeStampUnsafe           ),
      hasMobileOptOut               =   optional(   "hasMobileOptOut",                "hasMobileOptOut",              parseBoolUnsafe                    ),
      hasFaxOptIn                   =   optional(   "hasFaxOptIn",                    "hasFaxOptIn",                  parseBoolUnsafe                    ),
      hasFaxOptOut                  =   optional(   "hasFaxOptOut",                   "hasFaxOptOut",                 parseBoolUnsafe                    ),
      webUpdaterId                  = Option.empty,
      additionalFields              = additionalFields,
      ingestionErrors               = errors
    )
    // format: ON
  }
}
