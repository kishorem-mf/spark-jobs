package com.unilever.ohub.spark.tsv2parquet.file_interface

import com.unilever.ohub.spark.domain.Operator
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import com.unilever.ohub.spark.generic.StringFunctions._
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer
import org.apache.spark.sql.Row

object OperatorConverter extends FileDomainGateKeeper[Operator] {

  override def toDomainEntity: (Row, DomainTransformer) => Operator = {
    (row, transformer) =>
      import transformer._
      implicit val source: Row = row

      val countryCode: String = originalValue("COUNTRY_CODE")(row).getOrElse("")

      // @formatter:off             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

                                                                              // ↓ not so happy with this column (it should be the same as the fieldName), macro?
      Operator(
        // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
        sourceEntityId              = mandatory ( "﻿REF_OPERATOR_ID",         "sourceEntityId"                                                          ),
        sourceName                  = mandatory ( "SOURCE",                   "sourceName"                                                              ),
        countryCode                 = mandatory ( "COUNTRY_CODE",             "countryCode"                                                             ),
        isActive                    = mandatory ( "STATUS",                   "isActive",                     parseBoolUnsafe _                         ),
        name                        = mandatory ( "NAME",                     "name"                                                                    ),
        oldIntegrationId            = optional  ( "OPR_INTEGRATION_ID",       "oldIntegrationId"                                                        ),
        webUpdaterId                = None                                                                                                               , // TODO
        customerType                = None                                                                                                               , // TODO
        dateCreated                 = optional  ( "DATE_CREATED",             "dateCreated",                  parseDateTimeStampUnsafe _                ),
        dateUpdated                 = optional  ( "DATE_MODIFIED",            "dateUpdated",                  parseDateTimeStampUnsafe _                ),
        ohubCreated                 = None                                                                                                               , // TODO
        ohubUpdated                 = None                                                                                                               , // TODO
        channel                     = optional  ( "CHANNEL",                  "channel"                                                                 ),
        subChannel                  = optional  ( "SUB_CHANNEL",              "subChannel"                                                              ),
        region                      = optional  ( "REGION",                   "region"                                                                  ),
        street                      = optional  ( "STREET",                   "street",                       removeSpacesStrangeCharsAndToLower _      ),
        houseNumber                 = optional  ( "HOUSENUMBER",              "houseNumber"                                                             ),
        houseNumberExtension        = optional  ( "HOUSENUMBER_EXT",          "houseNumberExtension"                                                    ),
        city                        = optional  ( "CITY",                     "city",                         removeSpacesStrangeCharsAndToLower _      ),
        zipCode                     = optional  ( "ZIP_CODE",                 "zipCode",                      removeSpacesStrangeCharsAndToLower _      ),
        state                       = optional  ( "STATE",                    "state"                                                                   ),
        countryName                 = optional  ( "COUNTRY",                  "countryName"                                                             ),
        emailAddress                = optional  ( "EMAIL_ADDRESS",            "emailAddress"                                                            ),
        phoneNumber                 = optional  ( "PHONE_NUMBER",             "phoneNumber",                  cleanPhone(countryCode) _                 ),
        mobileNumber                = optional  ( "MOBILE_PHONE_NUMBER",      "mobilePhoneNumber",            cleanPhone(countryCode) _                 ),
        faxNumber                   = optional  ( "FAX_NUMBER",               "faxNumber",                    cleanPhone(countryCode) _                 ),
        hasGeneralOptOut            = optional  ( "OPT_OUT",                  "generalOptOut",                parseBoolUnsafe _                         ),
        hasEmailOptIn               = optional  ( "EM_OPT_IN",                "emailOptIn",                   parseBoolUnsafe _                         ),
        hasEmailOptOut              = optional  ( "EM_OPT_OUT",               "emailOptOut",                  parseBoolUnsafe _                         ),
        hasDirectMailOptIn          = optional  ( "DM_OPT_IN",                "directMailOptIn",              parseBoolUnsafe _                         ),
        hasDirectMailOptOut         = optional  ( "DM_OPT_OUT",               "directMailOptOut",             parseBoolUnsafe _                         ),
        hasTelemarketingOptIn       = optional  ( "TM_OPT_IN",                "telemarketingOptIn",           parseBoolUnsafe _                         ),
        hasTelemarketingOptOut      = optional  ( "TM_OPT_OUT",               "telemarketingOptOut",          parseBoolUnsafe _                         ),
        hasMobileOptIn              = optional  ( "MOB_OPT_IN",               "mobileOptIn",                  parseBoolUnsafe _                         ),
        hasMobileOptOut             = optional  ( "MOB_OPT_OUT",              "mobileOptOut",                 parseBoolUnsafe _                         ),
        hasFaxOptIn                 = optional  ( "FAX_OPT_IN",               "faxOptIn",                     parseBoolUnsafe _                         ),
        hasFaxOptOut                = optional  ( "FAX_OPT_OUT",              "faxOptOut",                    parseBoolUnsafe _                         ),
        totalDishes                 = optional  ( "NR_OF_DISHES",             "totalDishes",                  parseNumberOrAverageFromRange _           ),
        totalLocations              = optional  ( "NR_OF_LOCATIONS",          "totalLocations",               parseNumberOrAverageFromRange _           ),
        totalStaff                  = optional  ( "NR_OF_STAFF",              "totalStaff",                   parseNumberOrAverageFromRange _           ),
        averagePrice                = optional  ( "AVG_PRICE",                "averagePrice",                 parseBigDecimalOrAverageFromRange _       ),
        daysOpen                    = optional  ( "DAYS_OPEN",                "daysOpen",                     toInt _                                   ),
        weeksClosed                 = optional  ( "WEEKS_CLOSED",             "weeksClosed",                  toInt _                                   ),
        distributorName             = optional  ( "DISTRIBUTOR_NAME",         "distributorName"                                                         ),
        distributorCustomerNumber   = optional  ( "DISTRIBUTOR_CUSTOMER_NR",  "distributorCustomerNumber"                                               ),
        distributorOperatorId       = None                                                                                                               , // TODO not in row input, is this correct?
        otm                         = optional  ( "OTM",                      "otm"                                                                     ),
        otmEnteredBy                = optional  ( "OTM_REASON",               "otmEnteredBy"                                                            ), // TODO verify
        isNotRecalculatingOtm       = optional  ( "OTM_DNR",                  "isNotRecalculatingOtm",        parseBoolUnsafe _                         ),
        netPromoterScore            = optional  ( "NPS_POTENTIAL",            "netPromoterScore"                                                        ),
        salesRepresentative         = optional  ( "SALES_REP",                "salesRepresentative"                                                     ),
        cookingConvenienceLevel     = optional  ( "CONVENIENCE_LEVEL",        "cookingConvenienceLevel"                                                 ),
        isPrivateHousehold          = optional  ( "PRIVATE_HOUSEHOLD",        "isPrivateHousehold",           parseBoolUnsafe _                         ),
        vat                         = optional  ( "VAT_NUMBER",               "vat"                                                                     ),
        isOpenOnMonday              = optional  ( "OPEN_ON_MONDAY",           "isOpenOnMonday",               parseBoolUnsafe _                         ),
        isOpenOnTuesday             = optional  ( "OPEN_ON_TUESDAY",          "isOpenOnTuesday",              parseBoolUnsafe _                         ),
        isOpenOnWednesday           = optional  ( "OPEN_ON_WEDNESDAY",        "isOpenOnWednesday",            parseBoolUnsafe _                         ),
        isOpenOnThursday            = optional  ( "OPEN_ON_THURSDAY",         "isOpenOnThursday",             parseBoolUnsafe _                         ),
        isOpenOnFriday              = optional  ( "OPEN_ON_FRIDAY",           "isOpenOnFriday",               parseBoolUnsafe _                         ),
        isOpenOnSaturday            = optional  ( "OPEN_ON_SATURDAY",         "isOpenOnSaturday",             parseBoolUnsafe _                         ),
        isOpenOnSunday              = optional  ( "OPEN_ON_SUNDAY",           "isOpenOnSunday",               parseBoolUnsafe _                         ),
        chainName                   = optional  ( "CHAIN_NAME",               "chainName"                                                               ),
        chainId                     = optional  ( "CHAIN_ID",                 "chainId"                                                                 ),
        germanChainId               = None                                                                                                               ,
        germanChainName             = None                                                                                                               ,
        kitchenType                 = optional  ( "KITCHEN_TYPE",             "kitchenType"                                                             ),
        ingestionErrors             = errors
      )
      // @formatter:on
  }
}
