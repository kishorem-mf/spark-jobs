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
      implicit val r: Row = row

      // @formatter:off             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

                                                                              // ↓ not so happy with this column (it should be the same as the fieldName), macro?
      val v  = Operator(
        // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)           implicit vs explicit row arg passing, what to prefer?
        sourceOperatorId            = mandatory ( "﻿REF_OPERATOR_ID",         "sourceOperatorId"                                                        ),      // implicit
        sourceName                  = optional  ( "SOURCE",                   "sourceName"                                                              )(row), // explicit
        countryCode                 = optional  ( "COUNTRY_CODE",             "countryCode"                                                             )(row),
        isActive                    = optional  ( "STATUS",                   "isActive",                     parseBoolUnsafe _                         )(row),
        name                        = optional  ( "NAME",                     "name"                                                                    )(row),
        oldIntegrationId            = optional  ( "OPR_INTEGRATION_ID",       "oldIntegrationId"                                                        )(row),
        customerConcatId            = None                                                                                                                    , // TODO
        webUpdaterId                = None                                                                                                                    , // TODO
        customerType                = None                                                                                                                    , // TODO
        dateCreated                 = optional  ( "DATE_CREATED",             "dateCreated",                  parseDateTimeStampUnsafe _                )(row),
        dateUpdated                 = optional  ( "DATE_MODIFIED",            "dateModified",                 parseDateTimeStampUnsafe _                )(row),
        ohubCreated                 = None                                                                                                                    , // TODO
        ohubUpdated                 = None                                                                                                                    , // TODO
        channel                     = optional  ( "CHANNEL",                  "channel"                                                                 )(row),
        subChannel                  = optional  ( "SUB_CHANNEL",              "subChannel"                                                              )(row),
        region                      = optional  ( "REGION",                   "region"                                                                  )(row),
        street                      = optional  ( "STREET",                   "street",                       removeSpacesStrangeCharsAndToLower _      )(row),
        houseNumber                 = optional  ( "HOUSENUMBER",              "houseNumber"                                                             )(row),
        houseNumberExtension        = optional  ( "HOUSENUMBER_EXT",          "houseNumberExtension"                                                    )(row),
        city                        = optional  ( "CITY",                     "city",                         removeSpacesStrangeCharsAndToLower _      )(row),
        zipCode                     = optional  ( "ZIP_CODE",                 "zipCode",                      removeSpacesStrangeCharsAndToLower _      )(row),
        state                       = optional  ( "STATE",                    "state"                                                                   )(row),
        countryName                 = optional  ( "COUNTRY",                  "countryName"                                                             )(row),
        emailAddress                = optional  ( "EMAIL_ADDRESS",            "emailAddress"                                                            )(row),
        phoneNumber                 = optional  ( "PHONE_NUMBER",             "phoneNumber"                                                             )(row),
        mobileNumber                = optional  ( "MOBILE_PHONE_NUMBER",      "mobilePhoneNumber"                                                       )(row),
        faxNumber                   = optional  ( "FAX_NUMBER",               "faxNumber"                                                               )(row),
        generalOptOut               = optional  ( "OPT_OUT",                  "generalOptOut",                parseBoolUnsafe _                         )(row),
        emailOptIn                  = optional  ( "EM_OPT_IN",                "emailOptIn",                   parseBoolUnsafe _                         )(row),
        emailOptOut                 = optional  ( "EM_OPT_OUT",               "emailOptOut",                  parseBoolUnsafe _                         )(row),
        directMailOptIn             = optional  ( "DM_OPT_IN",                "directMailOptIn",              parseBoolUnsafe _                         )(row),
        directMailOptOut            = optional  ( "DM_OPT_OUT",               "directMailOptOut",             parseBoolUnsafe _                         )(row),
        telemarketingOptIn          = optional  ( "TM_OPT_IN",                "telemarketingOptIn",           parseBoolUnsafe _                         )(row),
        telemarketingOptOut         = optional  ( "TM_OPT_OUT",               "telemarketingOptOut",          parseBoolUnsafe _                         )(row),
        mobileOptIn                 = optional  ( "MOB_OPT_IN",               "mobileOptIn",                  parseBoolUnsafe _                         )(row),
        mobileOptOut                = optional  ( "MOB_OPT_OUT",              "mobileOptOut",                 parseBoolUnsafe _                         )(row),
        faxOptIn                    = optional  ( "FAX_OPT_IN",               "faxOptIn",                     parseBoolUnsafe _                         )(row),
        faxOptOut                   = optional  ( "FAX_OPT_OUT",              "faxOptOut",                    parseBoolUnsafe _                         )(row),
        totalDishes                 = optional  ( "NR_OF_DISHES",             "totalDishes",                  toLong _                                  )(row),
        totalLocations              = optional  ( "NR_OF_LOCATIONS",          "totalLocations",               toLong _                                  )(row),
        totalStaff                  = optional  ( "NR_OF_STAFF",              "totalStaff",                   toLong _                                  )(row),
        averagePrice                = optional  ( "AVG_PRICE",                "averagePrice",                 parseBigDecimalUnsafe _                   )(row),
        daysOpen                    = optional  ( "DAYS_OPEN",                "daysOpen",                     toLong _                                  )(row),
        weeksClosed                 = optional  ( "WEEKS_CLOSED",             "weeksClosed",                  toLong _                                  )(row),
        distributorName             = optional  ( "DISTRIBUTOR_NAME",         "distributorName"                                                         )(row),
        distributorCustomerNumber   = optional  ( "DISTRIBUTOR_CUSTOMER_NR",  "distributorCustomerNumber"                                               )(row),
        distributorOperatorId       = None                                                                                                                    , // TODO not in row input, is this correct?
        otm                         = optional  ( "OTM",                      "otm"                                                                     )(row),
        otmEnteredBy                = optional  ( "OTM_REASON",               "otmEnteredBy"                                                            )(row), // TODO verify
        isNotRecalculatingOtm       = optional  ( "OTM_DNR",                  "isNotRecalculatingOtm"                                                   )(row),
        netPromoterScore            = optional  ( "NPS_POTENTIAL",            "netPromoterScore"                                                        )(row),
        salesRepresentative         = optional  ( "SALES_REP",                "salesRepresentative"                                                     )(row),
        kookingConvenienceLevel     = optional  ( "CONVENIENCE_LEVEL",        "kookingConvenienceLevel"                                                 )(row),
        isPrivateHousehold          = optional  ( "PRIVATE_HOUSEHOLD",        "isPrivateHousehold"                                                      )(row),
        vat                         = optional  ( "VAT_NUMBER",               "vat"                                                                     )(row),
        isOpenOnMonday              = optional  ( "OPEN_ON_MONDAY",           "isOpenOnMonday",               parseBoolUnsafe _                         )(row),
        isOpenOnTuesday             = optional  ( "OPEN_ON_TUESDAY",          "isOpenOnTuesday",              parseBoolUnsafe _                         )(row),
        isOpenOnWednesday           = optional  ( "OPEN_ON_WEDNESDAY",        "isOpenOnWednesday",            parseBoolUnsafe _                         )(row),
        isOpenOnThursday            = optional  ( "OPEN_ON_THURSDAY",         "isOpenOnThursday",             parseBoolUnsafe _                         )(row),
        isOpenOnFriday              = optional  ( "OPEN_ON_FRIDAY",           "isOpenOnFriday",               parseBoolUnsafe _                         )(row),
        isOpenOnSaturday            = optional  ( "OPEN_ON_SATURDAY",         "isOpenOnSaturday",             parseBoolUnsafe _                         )(row),
        isOpenOnSunday              = optional  ( "OPEN_ON_SUNDAY",           "isOpenOnSunday",               parseBoolUnsafe _                         )(row),
        chainName                   = optional  ( "CHAIN_NAME",               "chainName"                                                               )(row),
        chainId                     = optional  ( "CHAIN_ID",                 "chainId"                                                                 )(row),
        germanChainId               = None                                                                                                                    , // TODO
        germanChainName             = None                                                                                                                    , // TODO
        kitchenType                 = optional  ( "KITCHEN_TYPE",             "kitchenType"                                                             )(row),
        ingestionErrors             = errors
      )
      // @formatter:on

      println(v)

      v
  }
}
