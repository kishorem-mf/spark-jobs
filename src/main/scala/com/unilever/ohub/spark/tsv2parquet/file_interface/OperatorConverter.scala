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

      // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

                                                                              // ↓ not so happy with this column (it should be the same as the fieldName), macro?
      Operator(
        // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
        concatId                    = concatId                                                                                                           ,
        countryCode                 = mandatory ( "COUNTRY_CODE",             "countryCode"                                                             ), // TODO lookup country code
        isActive                    = mandatory ( "STATUS",                   "isActive",                     parseBoolUnsafe _                         ),
        isGoldenRecord              = false                                                                                                              ,
        ohubId                     = Option.empty                                                                                                       ,
        name                        = mandatory ( "NAME",                     "name"                                                                    ),
        sourceEntityId              = mandatory ( "REF_OPERATOR_ID",          "sourceEntityId"                                                          ),
        sourceName                  = mandatory ( "SOURCE",                   "sourceName"                                                              ),
        ohubCreated                 = ohubCreated                                                                                                        ,
        ohubUpdated                 = ohubCreated                                                                                                        ,
        averagePrice                = optional  ( "AVG_PRICE",                "averagePrice",                 parseBigDecimalOrAverageFromRange _       ),
        chainId                     = optional  ( "CHAIN_ID",                 "chainId"                                                                 ),
        chainName                   = optional  ( "CHAIN_NAME",               "chainName"                                                               ),
        channel                     = optional  ( "CHANNEL",                  "channel"                                                                 ),
        city                        = optional  ( "CITY",                     "city"                                                                    ),
        cookingConvenienceLevel     = optional  ( "CONVENIENCE_LEVEL",        "cookingConvenienceLevel"                                                 ),
        countryName                 = optional  ( "COUNTRY",                  "countryName"                                                             ),
        customerType                = None                                                                                                               , // TODO introduce when enum is available
        dateCreated                 = optional  ( "DATE_CREATED",             "dateCreated",                  parseDateTimeStampUnsafe _                ),
        dateUpdated                 = optional  ( "DATE_MODIFIED",            "dateUpdated",                  parseDateTimeStampUnsafe _                ),
        daysOpen                    = optional  ( "DAYS_OPEN",                "daysOpen",                     toInt _                                   ),
        distributorCustomerNumber   = optional  ( "DISTRIBUTOR_CUSTOMER_NR",  "distributorCustomerNumber"                                               ),
        distributorName             = optional  ( "DISTRIBUTOR_NAME",         "distributorName"                                                         ),
        distributorOperatorId       = None                                                                                                               ,
        emailAddress                = optional  ( "EMAIL_ADDRESS",            "emailAddress"                                                            ),
        faxNumber                   = optional  ( "FAX_NUMBER",               "faxNumber",                    cleanPhone(countryCode) _                 ),
        hasDirectMailOptIn          = optional  ( "DM_OPT_IN",                "hasDirectMailOptIn",           parseBoolUnsafe _                         ),
        hasDirectMailOptOut         = optional  ( "DM_OPT_OUT",               "hasDirectMailOptOut",          parseBoolUnsafe _                         ),
        hasEmailOptIn               = optional  ( "EM_OPT_IN",                "hasEmailOptIn",                parseBoolUnsafe _                         ),
        hasEmailOptOut              = optional  ( "EM_OPT_OUT",               "hasEmailOptOut",               parseBoolUnsafe _                         ),
        hasFaxOptIn                 = optional  ( "FAX_OPT_IN",               "hasFaxOptIn",                  parseBoolUnsafe _                         ),
        hasFaxOptOut                = optional  ( "FAX_OPT_OUT",              "hasFaxOptOut",                 parseBoolUnsafe _                         ),
        hasGeneralOptOut            = optional  ( "OPT_OUT",                  "hasGeneralOptOut",             parseBoolUnsafe _                         ),
        hasMobileOptIn              = optional  ( "MOB_OPT_IN",               "hasMobileOptIn",               parseBoolUnsafe _                         ),
        hasMobileOptOut             = optional  ( "MOB_OPT_OUT",              "hasMobileOptOut",              parseBoolUnsafe _                         ),
        hasTelemarketingOptIn       = optional  ( "TM_OPT_IN",                "hasTelemarketingOptIn",        parseBoolUnsafe _                         ),
        hasTelemarketingOptOut      = optional  ( "TM_OPT_OUT",               "hasTelemarketingOptOut",       parseBoolUnsafe _                         ),
        houseNumber                 = optional  ( "HOUSENUMBER",              "houseNumber"                                                             ),
        houseNumberExtension        = optional  ( "HOUSENUMBER_EXT",          "houseNumberExtension"                                                    ),
        isNotRecalculatingOtm       = optional  ( "OTM_DNR",                  "isNotRecalculatingOtm",        parseBoolUnsafe _                         ),
        isOpenOnFriday              = optional  ( "OPEN_ON_FRIDAY",           "isOpenOnFriday",               parseBoolUnsafe _                         ),
        isOpenOnMonday              = optional  ( "OPEN_ON_MONDAY",           "isOpenOnMonday",               parseBoolUnsafe _                         ),
        isOpenOnSaturday            = optional  ( "OPEN_ON_SATURDAY",         "isOpenOnSaturday",             parseBoolUnsafe _                         ),
        isOpenOnSunday              = optional  ( "OPEN_ON_SUNDAY",           "isOpenOnSunday",               parseBoolUnsafe _                         ),
        isOpenOnThursday            = optional  ( "OPEN_ON_THURSDAY",         "isOpenOnThursday",             parseBoolUnsafe _                         ),
        isOpenOnTuesday             = optional  ( "OPEN_ON_TUESDAY",          "isOpenOnTuesday",              parseBoolUnsafe _                         ),
        isOpenOnWednesday           = optional  ( "OPEN_ON_WEDNESDAY",        "isOpenOnWednesday",            parseBoolUnsafe _                         ),
        isPrivateHousehold          = optional  ( "PRIVATE_HOUSEHOLD",        "isPrivateHousehold",           parseBoolUnsafe _                         ),
        kitchenType                 = optional  ( "KITCHEN_TYPE",             "kitchenType"                                                             ),
        mobileNumber                = optional  ( "MOBILE_PHONE_NUMBER",      "mobileNumber",                 cleanPhone(countryCode) _                 ),
        netPromoterScore            = optional  ( "NPS_POTENTIAL",            "netPromoterScore",             parseBigDecimalOrAverageFromRange _       ),
        oldIntegrationId            = optional  ( "OPR_INTEGRATION_ID",       "oldIntegrationId"                                                        ),
        otm                         = optional  ( "OTM",                      "otm"                                                                     ),
        otmEnteredBy                = optional  ( "OTM_REASON",               "otmEnteredBy"                                                            ),
        phoneNumber                 = optional  ( "PHONE_NUMBER",             "phoneNumber",                  cleanPhone(countryCode) _                 ),
        region                      = optional  ( "REGION",                   "region"                                                                  ),
        salesRepresentative         = optional  ( "SALES_REP",                "salesRepresentative"                                                     ),
        state                       = optional  ( "STATE",                    "state"                                                                   ),
        street                      = optional  ( "STREET",                   "street"                                                                  ),
        subChannel                  = optional  ( "SUB_CHANNEL",              "subChannel"                                                              ),
        totalDishes                 = optional  ( "NR_OF_DISHES",             "totalDishes",                  parseNumberOrAverageFromRange _           ),
        totalLocations              = optional  ( "NR_OF_LOCATIONS",          "totalLocations",               parseNumberOrAverageFromRange _           ),
        totalStaff                  = optional  ( "NR_OF_STAFF",              "totalStaff",                   parseNumberOrAverageFromRange _           ),
        vat                         = optional  ( "VAT_NUMBER",               "vat"                                                                     ),
        webUpdaterId                = None                                                                                                               ,
        weeksClosed                 = optional  ( "WEEKS_CLOSED",             "weeksClosed",                  toInt _                                   ),
        zipCode                     = optional  ( "ZIP_CODE",                 "zipCode"                                                                 ),
        ingestionErrors             = errors
      )
    // format: ON
  }
}
