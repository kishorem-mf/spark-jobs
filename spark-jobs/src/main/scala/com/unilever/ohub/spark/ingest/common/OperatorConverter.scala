package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, OperatorEmptyParquetWriter }
import org.apache.spark.sql.Row

object OperatorConverter extends CommonDomainGateKeeper[Operator] with OperatorEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Operator = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val countryCode = mandatoryValue("countryCode", "countryCode")(row)
    val concatId: String = createConcatId("countryCode", "sourceName", "sourceEntityId")
    val ohubCreated = currentTimestamp()

    // format: OFF

    Operator(
      // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      concatId                    = concatId                                                                                                           ,
      countryCode                 = countryCode                                                                                                        ,
      dateCreated                 = optional  ( "dateCreated",              "dateCreated",                  parseDateTimeStampUnsafe                  ),
      dateUpdated                 = optional  ( "dateUpdated",              "dateUpdated",                  parseDateTimeStampUnsafe                  ),
      customerType                = Operator.customerType                                                                                              ,
      isActive                    = mandatory ( "isActive",                 "isActive",                     parseBoolUnsafe                           ),
      isGoldenRecord              = false                                                                                                              ,
      ohubId                      = Option.empty,
      name                        = mandatory ( "name",                     "name"                                                                    ),
      sourceEntityId              = mandatory ( "sourceEntityId",           "sourceEntityId"                                                          ),
      sourceName                  = mandatory ( "sourceName",               "sourceName"                                                              ),
      ohubCreated                 = ohubCreated                                                                                                        ,
      ohubUpdated                 = ohubCreated                                                                                                        ,
      averagePrice                = optional  ( "averagePrice",             "averagePrice",                 parseBigDecimalOrAverageFromRange         ),
      chainId                     = optional  ( "chainId",                  "chainId"                                                                 ),
      chainName                   = optional  ( "chainName",                "chainName"                                                               ),
      channel                     = optional  ( "channel",                  "channel"                                                                 ),
      city                        = optional  ( "city",                     "city"                                                                    ),
      cookingConvenienceLevel     = optional  ( "cookingConvenienceLevel",  "cookingConvenienceLevel"                                                 ),
      countryName                 = countryName(countryCode)                                                                                           ,
      daysOpen                    = optional  ( "daysOpen",                 "daysOpen",                     withinRange(Operator.daysOpenRange)       ),
      distributorName             = optional  ( "distributorName",          "distributorName"                                                         ),
      distributorOperatorId       = optional  ( "distributorOperatorId",    "distributorOperatorId"                                                   ),
      emailAddress                = optional  ( "emailAddress",             "emailAddress",                 checkEmailValidity                        ),
      faxNumber                   = optional  ( "faxNumber",                "faxNumber",                    cleanPhone(countryCode)                   ),
      hasDirectMailOptIn          = optional  ( "hasDirectMailOptIn",       "hasDirectMailOptIn",           parseBoolUnsafe                           ),
      hasDirectMailOptOut         = optional  ( "hasDirectMailOptOut",      "hasDirectMailOptOut",          parseBoolUnsafe                           ),
      hasEmailOptIn               = optional  ( "hasEmailOptIn",            "hasEmailOptIn",                parseBoolUnsafe                           ),
      hasEmailOptOut              = optional  ( "hasEmailOptOut",           "hasEmailOptOut",               parseBoolUnsafe                           ),
      hasFaxOptIn                 = optional  ( "hasFaxOptIn",              "hasFaxOptIn",                  parseBoolUnsafe                           ),
      hasFaxOptOut                = optional  ( "hasFaxOptOut",             "hasFaxOptOut",                 parseBoolUnsafe                           ),
      hasGeneralOptOut            = optional  ( "hasGeneralOptOut",         "hasGeneralOptOut",             parseBoolUnsafe                           ),
      hasMobileOptIn              = optional  ( "hasMobileOptIn",           "hasMobileOptIn",               parseBoolUnsafe                           ),
      hasMobileOptOut             = optional  ( "hasMobileOptOut",          "hasMobileOptOut",              parseBoolUnsafe                           ),
      hasTelemarketingOptIn       = optional  ( "hasTelemarketingOptIn",    "hasTelemarketingOptIn",        parseBoolUnsafe                           ),
      hasTelemarketingOptOut      = optional  ( "hasTelemarketingOptOut",   "hasTelemarketingOptOut",       parseBoolUnsafe                           ),
      houseNumber                 = optional  ( "houseNumber",              "houseNumber"                                                             ),
      houseNumberExtension        = optional  ( "houseNumberExtension",     "houseNumberExtension"                                                    ),
      isNotRecalculatingOtm       = optional  ( "isNotRecalculatingOtm",    "isNotRecalculatingOtm",        parseBoolUnsafe                           ),
      isOpenOnFriday              = optional  ( "isOpenOnFriday",           "isOpenOnFriday",               parseBoolUnsafe                           ),
      isOpenOnMonday              = optional  ( "isOpenOnMonday",           "isOpenOnMonday",               parseBoolUnsafe                           ),
      isOpenOnSaturday            = optional  ( "isOpenOnSaturday",         "isOpenOnSaturday",             parseBoolUnsafe                           ),
      isOpenOnSunday              = optional  ( "isOpenOnSunday",           "isOpenOnSunday",               parseBoolUnsafe                           ),
      isOpenOnThursday            = optional  ( "isOpenOnThursday",         "isOpenOnThursday",             parseBoolUnsafe                           ),
      isOpenOnTuesday             = optional  ( "isOpenOnTuesday",          "isOpenOnTuesday",              parseBoolUnsafe                           ),
      isOpenOnWednesday           = optional  ( "isOpenOnWednesday",        "isOpenOnWednesday",            parseBoolUnsafe                           ),
      isPrivateHousehold          = optional  ( "isPrivateHousehold",       "isPrivateHousehold",           parseBoolUnsafe                           ),
      kitchenType                 = optional  ( "kitchenType",              "kitchenType"                                                             ),
      mobileNumber                = optional  ( "mobileNumber",             "mobileNumber",                 cleanPhone(countryCode)                   ),
      netPromoterScore            = optional  ( "netPromoterScore",         "netPromoterScore",             parseBigDecimalOrAverageFromRange         ),
      oldIntegrationId            = optional  ( "oldIntegrationId",         "oldIntegrationId"                                                        ),
      otm                         = optional  ( "otm",                      "otm",                          checkEnum(Operator.otmEnum)               ),
      otmEnteredBy                = optional  ( "otmEnteredBy",             "otmEnteredBy"                                                            ),
      phoneNumber                 = optional  ( "phoneNumber",              "phoneNumber",                  cleanPhone(countryCode)                   ),
      region                      = optional  ( "region",                   "region"                                                                  ),
      salesRepresentative         = optional  ( "salesRepresentative",      "salesRepresentative"                                                     ),
      state                       = optional  ( "state",                    "state"                                                                   ),
      street                      = optional  ( "street",                   "street"                                                                  ),
      subChannel                  = optional  ( "subChannel",               "subChannel"                                                              ),
      totalDishes                 = optional  ( "totalDishes",              "totalDishes",                  parseNumberOrAverageFromRange             ),
      totalLocations              = optional  ( "totalLocations",           "totalLocations",               parseNumberOrAverageFromRange             ),
      totalStaff                  = optional  ( "totalStaff",               "totalStaff",                   parseNumberOrAverageFromRange             ),
      vat                         = optional  ( "vat",                      "vat"                                                                     ),
      webUpdaterId                = None                                                                                                               ,
      weeksClosed                 = optional  ( "weeksClosed",              "weeksClosed",                  withinRange(Operator.weeksClosedRange)    ),
      zipCode                     = optional  ( "zipCode",                  "zipCode"                                                                 ),
      additionalFields            = additionalFields                                                                                                   ,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
