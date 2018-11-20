package com.unilever.ohub.spark.ingest.common

import com.unilever.ohub.spark.domain.entity.Operator
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
      id                          = mandatory( "id",                        "id"),
      creationTimestamp           = mandatory( "creationTimestamp",         "creationTimestamp",            toTimestamp),
      concatId                    = concatId                                                                                                           ,
      countryCode                 = countryCode                                                                                                        ,
      dateCreated                 = optional  ( "dateCreated",              "dateCreated",                  parseDateTimeUnsafe()                     ),
      dateUpdated                 = optional  ( "dateUpdated",              "dateUpdated",                  parseDateTimeUnsafe()                     ),
      customerType                = Operator.customerType                                                                                              ,
      isActive                    = mandatory ( "isActive",                 "isActive",                     toBoolean                                 ),
      isGoldenRecord              = false                                                                                                              ,
      ohubId                      = Option.empty,
      name                        = mandatory ( "name",                     "name"                                                                    ),
      sourceEntityId              = mandatory ( "sourceEntityId",           "sourceEntityId"                                                          ),
      sourceName                  = mandatory ( "sourceName",               "sourceName"                                                              ),
      ohubCreated                 = ohubCreated                                                                                                        ,
      ohubUpdated                 = ohubCreated                                                                                                        ,
      averagePrice                = optional  ( "averagePrice",             "averagePrice",                 toBigDecimal                              ),
      chainId                     = optional  ( "chainId",                  "chainId"                                                                 ),
      chainName                   = optional  ( "chainName",                "chainName"                                                               ),
      channel                     = optional  ( "channel",                  "channel"                                                                 ),
      city                        = optional  ( "city",                     "city"                                                                    ),
      cookingConvenienceLevel     = optional  ( "cookingConvenienceLevel",  "cookingConvenienceLevel"                                                 ),
      countryName                 = optional  ( "countryName",              "countryName"                                                             ),
      daysOpen                    = optional  ( "daysOpen",                 "daysOpen",                     toInt                                     ),
      distributorName             = optional  ( "distributorName",          "distributorName"                                                         ),
      distributorOperatorId       = optional  ( "distributorOperatorId",    "distributorOperatorId"                                                   ),
      emailAddress                = optional  ( "emailAddress",             "emailAddress"                                                            ),
      faxNumber                   = optional  ( "faxNumber",                "faxNumber"                                                               ),
      hasDirectMailOptIn          = optional  ( "hasDirectMailOptIn",       "hasDirectMailOptIn",           toBoolean                                 ),
      hasDirectMailOptOut         = optional  ( "hasDirectMailOptOut",      "hasDirectMailOptOut",          toBoolean                                 ),
      hasEmailOptIn               = optional  ( "hasEmailOptIn",            "hasEmailOptIn",                toBoolean                                 ),
      hasEmailOptOut              = optional  ( "hasEmailOptOut",           "hasEmailOptOut",               toBoolean                                 ),
      hasFaxOptIn                 = optional  ( "hasFaxOptIn",              "hasFaxOptIn",                  toBoolean                                 ),
      hasFaxOptOut                = optional  ( "hasFaxOptOut",             "hasFaxOptOut",                 toBoolean                                 ),
      hasGeneralOptOut            = optional  ( "hasGeneralOptOut",         "hasGeneralOptOut",             toBoolean                                 ),
      hasMobileOptIn              = optional  ( "hasMobileOptIn",           "hasMobileOptIn",               toBoolean                                 ),
      hasMobileOptOut             = optional  ( "hasMobileOptOut",          "hasMobileOptOut",              toBoolean                                 ),
      hasTelemarketingOptIn       = optional  ( "hasTelemarketingOptIn",    "hasTelemarketingOptIn",        toBoolean                                 ),
      hasTelemarketingOptOut      = optional  ( "hasTelemarketingOptOut",   "hasTelemarketingOptOut",       toBoolean                                 ),
      houseNumber                 = optional  ( "houseNumber",              "houseNumber"                                                             ),
      houseNumberExtension        = optional  ( "houseNumberExtension",     "houseNumberExtension"                                                    ),
      isNotRecalculatingOtm       = optional  ( "isNotRecalculatingOtm",    "isNotRecalculatingOtm",        toBoolean                                 ),
      isOpenOnFriday              = optional  ( "isOpenOnFriday",           "isOpenOnFriday",               toBoolean                                 ),
      isOpenOnMonday              = optional  ( "isOpenOnMonday",           "isOpenOnMonday",               toBoolean                                 ),
      isOpenOnSaturday            = optional  ( "isOpenOnSaturday",         "isOpenOnSaturday",             toBoolean                                 ),
      isOpenOnSunday              = optional  ( "isOpenOnSunday",           "isOpenOnSunday",               toBoolean                                 ),
      isOpenOnThursday            = optional  ( "isOpenOnThursday",         "isOpenOnThursday",             toBoolean                                 ),
      isOpenOnTuesday             = optional  ( "isOpenOnTuesday",          "isOpenOnTuesday",              toBoolean                                 ),
      isOpenOnWednesday           = optional  ( "isOpenOnWednesday",        "isOpenOnWednesday",            toBoolean                                 ),
      isPrivateHousehold          = optional  ( "isPrivateHousehold",       "isPrivateHousehold",           toBoolean                                 ),
      kitchenType                 = optional  ( "kitchenType",              "kitchenType"                                                             ),
      mobileNumber                = optional  ( "mobileNumber",             "mobileNumber"                                                            ),
      netPromoterScore            = optional  ( "netPromoterScore",         "netPromoterScore",             toBigDecimal                              ),
      oldIntegrationId            = optional  ( "oldIntegrationId",         "oldIntegrationId"                                                        ),
      otm                         = optional  ( "otm",                      "otm"                                                                     ),
      otmEnteredBy                = optional  ( "otmEnteredBy",             "otmEnteredBy"                                                            ),
      phoneNumber                 = optional  ( "phoneNumber",              "phoneNumber"                                                             ),
      region                      = optional  ( "region",                   "region"                                                                  ),
      salesRepresentative         = optional  ( "salesRepresentative",      "salesRepresentative"                                                     ),
      state                       = optional  ( "state",                    "state"                                                                   ),
      street                      = optional  ( "street",                   "street"                                                                  ),
      subChannel                  = optional  ( "subChannel",               "subChannel"                                                              ),
      totalDishes                 = optional  ( "totalDishes",              "totalDishes",                  toInt                                     ),
      totalLocations              = optional  ( "totalLocations",           "totalLocations",               toInt                                     ),
      totalStaff                  = optional  ( "totalStaff",               "totalStaff",                   toInt                                     ),
      vat                         = optional  ( "vat",                      "vat"                                                                     ),
      webUpdaterId                = None                                                                                                               ,
      weeksClosed                 = optional  ( "weeksClosed",              "weeksClosed",                  toInt                                     ),
      zipCode                     = optional  ( "zipCode",                  "zipCode"                                                                 ),
      additionalFields            = additionalFields                                                                                                   ,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
