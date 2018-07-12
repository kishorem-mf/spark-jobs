package com.unilever.ohub.spark.ingest.web_event_interface

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.generic.StringFunctions.{ checkEmailValidity, checkEnum, cleanPhone }
import com.unilever.ohub.spark.ingest.{ ContactPersonEmptyParquetWriter, DomainTransformer }
import org.apache.spark.sql.Row
import cats.syntax.option._

object ContactPersonConverter extends WebEventDomainGateKeeper[ContactPerson] with ContactPersonEmptyParquetWriter {
  override protected def toDomainEntity: DomainTransformer ⇒ Row ⇒ ContactPerson = { transformer ⇒ implicit row ⇒
    import transformer._

    val countryCode: String = mandatoryValue("countryCode", "countryCode")
    val sourceEntityId: String = mandatoryValue("sourceId", "sourceEntityId")
    val concatId: String = DomainEntity.createConcatIdFromValues(countryCode, SourceName, sourceEntityId)
    val name: String = concatValues("firstName", "lastName")
    val ohubCreated: Timestamp = currentTimestamp()

    // format: OFF

    ContactPerson(
      concatId                      = concatId                                                                                                            ,
      countryCode                   = countryCode                                                                                                         ,
      customerType                  = ContactPerson.customerType                                                                                          ,
      ohubCreated                   = ohubCreated                                                                                                         ,
      ohubUpdated                   = ohubCreated                                                                                                         ,
      ohubId                        = none                                                                                                        ,
      isGoldenRecord                = false                                                                                                               ,
      sourceEntityId                =   mandatory(  "sourceId",          "sourceEntityId"),
      sourceName                    = SourceName,
      isActive                      = true,
      dateCreated                   = none,
      dateUpdated                   = none,
      name                          = name                                                                                                                ,
      operatorConcatId              = concatId                                                                                                            ,
      operatorOhubId                = none,
      oldIntegrationId              = none,
      firstName                     =   optional(   "firstName",                      "firstName"                                                        ),
      lastName                      =   optional(   "lastName",                       "lastName"                                                         ),
      title                         =   optional(   "title",                          "title"                                                            ),
      gender                        =   optional(   "gender",                         "gender",                       checkEnum(ContactPerson.genderEnum) ),
      jobTitle                      =   optional(   "jobTitle",                       "jobTitle"                                                         ),
      language                      =   optional(   "language",                       "language"                                                         ),
      birthDate                     = none,
      street                        = none,
      houseNumber                   = none,
      houseNumberExtension          = none,
      city                          = none,
      zipCode                       = none,
      state                         = none,
      countryName                   =   countryName(countryCode)                                                                                          ,
      isPreferredContact            = none,
      isKeyDecisionMaker            = none,
      standardCommunicationChannel  = none,
      emailAddress                  =   optional(   "emailAddress",                   "emailAddress",                 checkEmailValidity                 ),
      phoneNumber                   =   optional(   "phone",                          "phoneNumber",                  cleanPhone(countryCode)            ),
      mobileNumber                  =   optional(   "mobilePhone",                    "mobileNumber",                 cleanPhone(countryCode)            ),
      faxNumber                     =   optional(   "fax",                            "faxNumber",                    cleanPhone(countryCode)            ),
      hasGeneralOptOut              = none,
      hasConfirmedRegistration      = none,
      confirmedRegistrationDate     = none,
      hasEmailOptIn                 = none, // look up in subscription table
      emailOptInDate                = none, // look up in subscription table
      hasEmailDoubleOptIn           = none, // look up in subscription table
      emailDoubleOptInDate          = none, // look up in subscription table
      hasEmailOptOut                = none,
      hasDirectMailOptIn            = none, // look up in subscription table
      hasDirectMailOptOut           = none,
      hasTeleMarketingOptIn         = none, // look up in subscription table
      hasTeleMarketingOptOut        = none,
      hasMobileOptIn                = none, // look up in subscription table
      mobileOptInDate               = none, // look up in subscription table
      hasMobileDoubleOptIn          = none, // look up in subscription table
      mobileDoubleOptInDate         = none, // look up in subscription table
      hasMobileOptOut               = none,
      hasFaxOptIn                   = none, // look up in subscription table
      hasFaxOptOut                  = none,
      webUpdaterId                  = none,
      additionalFields              = additionalFields,
      ingestionErrors               = errors
    )

    // format: ON
  }
}
