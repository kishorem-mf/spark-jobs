package com.unilever.ohub.spark.tsv2parquet.file_interface

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer
import org.apache.spark.sql.Row

object OperatorConverter extends FileDomainGateKeeper[Operator] {

  override def toDomainEntity: (Row, DomainTransformer) ⇒ Operator = {
    (row, transformer) ⇒
      import transformer._
      implicit val source: Row = row

      val countryCode: String = originalValue("COUNTRY_CODE")(row).get
      val concatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_OPERATOR_ID")
      val ohubCreated = currentTimestamp()

      // format: OFF
                                                                              // ↓ not so happy with this column (it should be the same as the fieldName), macro?
      Operator(
        // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
        concatId                    = concatId                                                                                                           ,
        countryCode                 = mandatory ( "COUNTRY_CODE",             "countryCode"                                                             ), // TODO lookup country code
        dateCreated                 = optional  ( "DATE_CREATED",             "dateCreated",                  parseDateTimeStampUnsafe                  ),
        dateUpdated                 = optional  ( "DATE_MODIFIED",            "dateUpdated",                  parseDateTimeStampUnsafe                  ),
        customerType                = Operator.customerType                                                                                              ,
        isActive                    = mandatory ( "STATUS",                   "isActive",                     parseBoolUnsafe                           ),
        isGoldenRecord              = false                                                                                                              ,
        ohubId                      = Option.empty                                                                                                       ,
        name                        = mandatory ( "NAME",                     "name"                                                                    ),
        sourceEntityId              = mandatory ( "REF_OPERATOR_ID",          "sourceEntityId"                                                          ),
        sourceName                  = mandatory ( "SOURCE",                   "sourceName"                                                              ),
        ohubCreated                 = ohubCreated                                                                                                        ,
        ohubUpdated                 = ohubCreated                                                                                                        ,
        averagePrice                = optional  ( "AVG_PRICE",                "averagePrice",                 parseBigDecimalOrAverageFromRange         ),
        chainId                     = optional  ( "CHAIN_ID",                 "chainId"                                                                 ),
        chainName                   = optional  ( "CHAIN_NAME",               "chainName"                                                               ),
        channel                     = optional  ( "CHANNEL",                  "channel"                                                                 ),
        city                        = optional  ( "CITY",                     "city"                                                                    ),
        cookingConvenienceLevel     = optional  ( "CONVENIENCE_LEVEL",        "cookingConvenienceLevel"                                                 ),
        countryName                 = optional  ( "COUNTRY",                  "countryName"                                                             ), // TODO derive from country code
        daysOpen                    = optional  ( "DAYS_OPEN",                "daysOpen",                     withinRange(Operator.daysOpenRange)       ),
        distributorName             = optional  ( "DISTRIBUTOR_NAME",         "distributorName"                                                         ),
        distributorOperatorId       = optional  ( "DISTRIBUTOR_CUSTOMER_NR",  "distributorOperatorId"                                                   ),
        emailAddress                = optional  ( "EMAIL_ADDRESS",            "emailAddress"                                                            ),
        faxNumber                   = optional  ( "FAX_NUMBER",               "faxNumber",                    cleanPhone(countryCode)                   ),
        hasDirectMailOptIn          = optional  ( "DM_OPT_IN",                "hasDirectMailOptIn",           parseBoolUnsafe                           ),
        hasDirectMailOptOut         = optional  ( "DM_OPT_OUT",               "hasDirectMailOptOut",          parseBoolUnsafe                           ),
        hasEmailOptIn               = optional  ( "EM_OPT_IN",                "hasEmailOptIn",                parseBoolUnsafe                           ),
        hasEmailOptOut              = optional  ( "EM_OPT_OUT",               "hasEmailOptOut",               parseBoolUnsafe                           ),
        hasFaxOptIn                 = optional  ( "FAX_OPT_IN",               "hasFaxOptIn",                  parseBoolUnsafe                           ),
        hasFaxOptOut                = optional  ( "FAX_OPT_OUT",              "hasFaxOptOut",                 parseBoolUnsafe                           ),
        hasGeneralOptOut            = optional  ( "OPT_OUT",                  "hasGeneralOptOut",             parseBoolUnsafe                           ),
        hasMobileOptIn              = optional  ( "MOB_OPT_IN",               "hasMobileOptIn",               parseBoolUnsafe                           ),
        hasMobileOptOut             = optional  ( "MOB_OPT_OUT",              "hasMobileOptOut",              parseBoolUnsafe                           ),
        hasTelemarketingOptIn       = optional  ( "TM_OPT_IN",                "hasTelemarketingOptIn",        parseBoolUnsafe                           ),
        hasTelemarketingOptOut      = optional  ( "TM_OPT_OUT",               "hasTelemarketingOptOut",       parseBoolUnsafe                           ),
        houseNumber                 = optional  ( "HOUSENUMBER",              "houseNumber"                                                             ),
        houseNumberExtension        = optional  ( "HOUSENUMBER_EXT",          "houseNumberExtension"                                                    ),
        isNotRecalculatingOtm       = optional  ( "OTM_DNR",                  "isNotRecalculatingOtm",        parseBoolUnsafe                           ),
        isOpenOnFriday              = optional  ( "OPEN_ON_FRIDAY",           "isOpenOnFriday",               parseBoolUnsafe                           ),
        isOpenOnMonday              = optional  ( "OPEN_ON_MONDAY",           "isOpenOnMonday",               parseBoolUnsafe                           ),
        isOpenOnSaturday            = optional  ( "OPEN_ON_SATURDAY",         "isOpenOnSaturday",             parseBoolUnsafe                           ),
        isOpenOnSunday              = optional  ( "OPEN_ON_SUNDAY",           "isOpenOnSunday",               parseBoolUnsafe                           ),
        isOpenOnThursday            = optional  ( "OPEN_ON_THURSDAY",         "isOpenOnThursday",             parseBoolUnsafe                           ),
        isOpenOnTuesday             = optional  ( "OPEN_ON_TUESDAY",          "isOpenOnTuesday",              parseBoolUnsafe                           ),
        isOpenOnWednesday           = optional  ( "OPEN_ON_WEDNESDAY",        "isOpenOnWednesday",            parseBoolUnsafe                           ),
        isPrivateHousehold          = optional  ( "PRIVATE_HOUSEHOLD",        "isPrivateHousehold",           parseBoolUnsafe                           ),
        kitchenType                 = optional  ( "KITCHEN_TYPE",             "kitchenType"                                                             ),
        mobileNumber                = optional  ( "MOBILE_PHONE_NUMBER",      "mobileNumber",                 cleanPhone(countryCode)                   ),
        netPromoterScore            = optional  ( "NPS_POTENTIAL",            "netPromoterScore",             parseBigDecimalOrAverageFromRange         ),
        oldIntegrationId            = optional  ( "OPR_INTEGRATION_ID",       "oldIntegrationId"                                                        ),
        otm                         = optional  ( "OTM",                      "otm"                                                                     ),
        otmEnteredBy                = optional  ( "OTM_REASON",               "otmEnteredBy"                                                            ),
        phoneNumber                 = optional  ( "PHONE_NUMBER",             "phoneNumber",                  cleanPhone(countryCode)                   ),
        region                      = optional  ( "REGION",                   "region"                                                                  ),
        salesRepresentative         = optional  ( "SALES_REP",                "salesRepresentative"                                                     ),
        state                       = optional  ( "STATE",                    "state"                                                                   ),
        street                      = optional  ( "STREET",                   "street"                                                                  ),
        subChannel                  = optional  ( "SUB_CHANNEL",              "subChannel"                                                              ),
        totalDishes                 = optional  ( "NR_OF_DISHES",             "totalDishes",                  parseNumberOrAverageFromRange             ),
        totalLocations              = optional  ( "NR_OF_LOCATIONS",          "totalLocations",               parseNumberOrAverageFromRange             ),
        totalStaff                  = optional  ( "NR_OF_STAFF",              "totalStaff",                   parseNumberOrAverageFromRange             ),
        vat                         = optional  ( "VAT_NUMBER",               "vat"                                                                     ),
        webUpdaterId                = None                                                                                                               ,
        weeksClosed                 = optional  ( "WEEKS_CLOSED",             "weeksClosed",                  withinRange(Operator.weeksClosedRange)    ),
        zipCode                     = optional  ( "ZIP_CODE",                 "zipCode"                                                                 ),
        additionalFields            = additionalFields                                                                                                   ,
        ingestionErrors             = errors
      )
    // format: ON
  }
}
