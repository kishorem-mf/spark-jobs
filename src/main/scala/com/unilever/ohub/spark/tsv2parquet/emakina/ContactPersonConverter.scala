package com.unilever.ohub.spark.tsv2parquet.emakina

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.ContactPerson
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import com.unilever.ohub.spark.tsv2parquet.{ ContactPersonEmptyParquetWriter, DomainTransformer }
import org.apache.spark.sql.Row

object ContactPersonConverter extends EmakinaDomainGateKeeper[ContactPerson] with ContactPersonEmptyParquetWriter {

  override protected[tsv2parquet] def toDomainEntity: DomainTransformer ⇒ Row ⇒ ContactPerson = { transformer ⇒ row ⇒
    import transformer._

    implicit val source: Row = row

    // format: OFF

    val countryCode       = mandatoryValue("COUNTRY_CODE", "countryCode")(row)
    val sourceEntityId    = mandatoryValue("EM_SOURCE_ID", "sourceEntityId")(row)
    val operatorRefId     = mandatoryValue("OPERATOR_REF_ID", "operatorConcatId")(row)
    val concatId          = DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
    val operatorConcatId  = DomainEntity.createConcatIdFromValues(countryCode, sourceName, operatorRefId)
    val name              = concatValues("FIRST_NAME", "LAST_NAME")
    val ohubCreated       = currentTimestamp()

    ContactPerson(
      concatId                      = concatId                                                                                                            ,
      countryCode                   = countryCode                                                                                                         ,
      customerType                  = ContactPerson.customerType                                                                                          ,
      ohubCreated                   = ohubCreated                                                                                                         ,
      ohubUpdated                   = ohubCreated                                                                                                         ,
      ohubId                        = Option.empty                                                                                                        ,
      isGoldenRecord                = false                                                                                                               ,
      sourceEntityId                = sourceEntityId                                                                                                      ,
      sourceName                    = sourceName                                                                                                          ,
      isActive                      = true                                                                                                                ,
      dateCreated                   = None                                                                                                                ,
      dateUpdated                   = None                                                                                                                ,
      name                          = name                                                                                                                ,
      operatorConcatId              = operatorConcatId                                                                                                    ,
      operatorOhubId                = None                                                                                                                ,
      oldIntegrationId              = None                                                                                                                ,
      firstName                     =   optional(   "FIRST_NAME",                     "firstName"                                                        ),
      lastName                      =   optional(   "LAST_NAME",                      "lastName"                                                         ),
      title                         =   optional(   "TITLE",                          "title"                                                            ),
      gender                        =   optional(   "GENDER",                         "gender"                                                           ),
      jobTitle                      =   optional(   "JOB_TITLE",                      "jobTitle"                                                         ),
      language                      =   optional(   "LANGUAGE",                       "language"                                                         ),
      birthDate                     = None                                                                                                                ,
      street                        =   optional(   "STREET",                         "street"                                                           ),
      houseNumber                   =   optional(   "HOUSE_NUMBER",                   "houseNumber"                                                      ),
      houseNumberExtension          =   optional(   "HOUSE_NUMBER_EXT",               "houseNumberExtension"                                             ),
      city                          =   optional(   "CITY",                           "city"                                                             ),
      zipCode                       =   optional(   "POSTCODE",                       "zipCode"                                                          ),
      state                         =   optional(   "STATE",                          "state"                                                            ),
      countryName                   = countryName(countryCode)                                                                                            ,
      isPreferredContact            = None                                                                                                                ,
      isKeyDecisionMaker            = None                                                                                                                ,
      standardCommunicationChannel  = None                                                                                                                ,
      emailAddress                  =   optional(   "EMAIL_ADDRESS",                  "emailAddress",                 checkEmailValidity                 ),
      phoneNumber                   =   optional(   "PHONE",                          "phoneNumber",                  cleanPhone(countryCode)            ),
      mobileNumber                  =   optional(   "MOBILE_PHONE",                   "mobileNumber",                 cleanPhone(countryCode)            ),
      faxNumber                     =   optional(   "FAX",                            "faxNumber",                    cleanPhone(countryCode)            ),
      hasGeneralOptOut              = None                                                                                                                ,
      hasConfirmedRegistration      = None                                                                                                                ,
      confirmedRegistrationDate     = None                                                                                                                ,
      hasEmailOptIn                 = None                                                                                                                ,
      emailOptInDate                =   optional(   "OPT_IN_DATE",                    "emailOptInDate",               parseDateTimeStampUnsafe           ),
      hasEmailDoubleOptIn           =   optional(   "CONFIRMED_OPT_IN",               "hasEmailDoubleOptIn",          parseBoolUnsafe                    ),
      emailDoubleOptInDate          =   optional(   "CONFIRMED_OPT_IN_DATE",          "emailDoubleOptInDate",         parseDateTimeStampUnsafe           ),
      hasEmailOptOut                = None                                                                                                                ,
      hasDirectMailOptIn            = None                                                                                                                ,
      hasDirectMailOptOut           = None                                                                                                                ,
      hasTeleMarketingOptIn         = None                                                                                                                ,
      hasTeleMarketingOptOut        = None                                                                                                                ,
      hasMobileOptIn                = None                                                                                                                ,
      mobileOptInDate               = None                                                                                                                ,
      hasMobileDoubleOptIn          = None                                                                                                                ,
      mobileDoubleOptInDate         = None                                                                                                                ,
      hasMobileOptOut               = None                                                                                                                ,
      hasFaxOptIn                   = None                                                                                                                ,
      hasFaxOptOut                  = None                                                                                                                ,
      webUpdaterId                  =   optional(   "WEBUPDATER_ID",                  "webUpdaterId"                                                     ),
      additionalFields              = additionalFields                                                                                                    ,
      ingestionErrors               = errors
    )
    // format: ON
  }
}
