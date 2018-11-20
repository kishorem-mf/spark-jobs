package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.ContactPerson
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
    val ohubCreated       = currentTimestamp()

    // fieldName                        mandatory   sourceFieldName                   targetFieldName                 transformationFunction (unsafe)
    ContactPerson(
      id                            = mandatory(    "id",                             "id"),
      creationTimestamp             = mandatory(    "creationTimestamp",              "creationTimestamp",            toTimestamp),
      concatId                      = concatId                                                                                                            ,
      countryCode                   = countryCode                                                                                                         ,
      customerType                  = ContactPerson.customerType                                                                                          ,
      ohubCreated                   = ohubCreated                                                                                                         ,
      ohubUpdated                   = ohubCreated                                                                                                         ,
      ohubId                        = Option.empty, // set in ContactPersonMatchingJoiner, ContactPersonExactMatcher, ContactPersonIntegratedExactMatch
      isGoldenRecord                = false                                                                                                               ,
      sourceEntityId                =   mandatory(  "sourceEntityId",                 "sourceEntityId"                                                   ),
      sourceName                    =   mandatory(  "sourceName",                     "sourceName"                                                       ),
      isActive                      =   mandatory(	"isActive",                       "isActive",                     toBoolean                          ),
      dateCreated                   =   optional(   "dateCreated",                    "dateCreated",                  parseDateTimeUnsafe()              ),
      dateUpdated                   =   optional(   "dateUpdated",                    "dateUpdated",                  parseDateTimeUnsafe()              ),
      operatorConcatId              =   mandatory(  "operatorConcatId",               "operatorConcatId"                                                 ),
      operatorOhubId                = Option.empty,  // set in ContactPersonReferencing
      oldIntegrationId              =   optional(   "oldIntegrationId",               "oldIntegrationId"                                                 ),
      firstName                     =   optional(   "firstName",                      "firstName"                                                        ),
      lastName                      =   optional(   "lastName",                       "lastName"                                                         ),
      title                         =   optional(   "title",                          "title"                                                            ),
      gender                        =   optional(   "gender",                         "gender"                                                           ),
      jobTitle                      =   optional(   "jobTitle",                       "jobTitle"                                                         ),
      language                      =   optional(   "language",                       "language"                                                         ),
      birthDate                     =   optional(   "birthDate",                      "birthDate",                    parseDateUnsafe()                  ),
      street                        =   optional(   "street",                         "street"                                                           ),
      houseNumber                   =   optional(   "houseNumber",                    "houseNumber"                                                      ),
      houseNumberExtension          =   optional(   "houseNumberExtension",           "houseNumberExtension"                                             ),
      city                          =   optional(   "city",                           "city"                                                             ),
      zipCode                       =   optional(   "zipCode",                        "zipCode"                                                          ),
      state                         =   optional(   "state",                          "state"                                                            ),
      countryName                   =   optional(   "countryName",                    "countryName"                                                      ),
      isPreferredContact            =   optional(   "isPreferredContact",             "isPreferredContact",           toBoolean                          ),
      isKeyDecisionMaker            =   optional(   "isKeyDecisionMaker",             "isKeyDecisionMaker",           toBoolean                          ),
      standardCommunicationChannel  =   optional(   "standardCommunicationChannel",   "standardCommunicationChannel"                                     ),
      emailAddress                  =   optional(   "emailAddress",                   "emailAddress"                                                     ),
      phoneNumber                   =   optional(   "phoneNumber",                    "phoneNumber"                                                      ),
      mobileNumber                  =   optional(   "mobileNumber",                   "mobileNumber"                                                     ),
      faxNumber                     =   optional(   "faxNumber",                      "faxNumber"                                                        ),
      hasGeneralOptOut              =   optional(   "hasGeneralOptOut",               "hasGeneralOptOut",             toBoolean                          ),
      hasConfirmedRegistration      =   optional(   "hasConfirmedRegistration",       "hasConfirmedRegistration",     toBoolean                          ),
      confirmedRegistrationDate     =   optional(   "confirmedRegistrationDate",      "confirmedRegistrationDate",    parseDateTimeUnsafe()              ),
      hasEmailOptIn                 =   optional(   "hasEmailOptIn",                  "hasEmailOptIn",                toBoolean                          ),
      emailOptInDate                =   optional(   "emailOptInDate",                 "emailOptInDate",               parseDateTimeUnsafe()              ),
      hasEmailDoubleOptIn           =   optional(   "hasEmailDoubleOptIn",            "hasEmailDoubleOptIn",          toBoolean                          ),
      emailDoubleOptInDate          =   optional(   "emailDoubleOptInDate",           "emailDoubleOptInDate",         parseDateTimeUnsafe()              ),
      hasEmailOptOut                =   optional(   "hasEmailOptOut",                 "hasEmailOptOut",               toBoolean                          ),
      hasDirectMailOptIn            =   optional(   "hasDirectMailOptIn",             "hasDirectMailOptIn",           toBoolean                          ),
      hasDirectMailOptOut           =   optional(   "hasDirectMailOptOut",            "hasDirectMailOptOut",          toBoolean                          ),
      hasTeleMarketingOptIn         =   optional(   "hasTeleMarketingOptIn",          "hasTeleMarketingOptIn",        toBoolean                          ),
      hasTeleMarketingOptOut        =   optional(   "hasTeleMarketingOptOut",         "hasTeleMarketingOptOut",       toBoolean                          ),
      hasMobileOptIn                =   optional(   "hasMobileOptIn",                 "hasMobileOptIn",               toBoolean                          ),
      mobileOptInDate               =   optional(   "mobileOptInDate",                "mobileOptInDate",              parseDateTimeUnsafe()              ),
      hasMobileDoubleOptIn          =   optional(   "hasMobileDoubleOptIn",           "hasMobileDoubleOptIn",         toBoolean                          ),
      mobileDoubleOptInDate         =   optional(   "mobileDoubleOptInDate",          "mobileDoubleOptInDate",        parseDateTimeUnsafe()              ),
      hasMobileOptOut               =   optional(   "hasMobileOptOut",                "hasMobileOptOut",              toBoolean                          ),
      hasFaxOptIn                   =   optional(   "hasFaxOptIn",                    "hasFaxOptIn",                  toBoolean                          ),
      hasFaxOptOut                  =   optional(   "hasFaxOptOut",                   "hasFaxOptOut",                 toBoolean                          ),
      webUpdaterId                  = Option.empty, // TODO what to do with this one?
      additionalFields              = additionalFields,
      ingestionErrors               = errors
    )
    // format: ON
  }
}
