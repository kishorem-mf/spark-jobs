package com.unilever.ohub.spark.tsv2parquet.emakina

import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer
import org.apache.spark.sql.Row

object OperatorConverter extends EmakinaDomainGateKeeper[Operator] {

  override def toDomainEntity: (Row, DomainTransformer) ⇒ Operator = {
    (row, transformer) ⇒
      import transformer._
      implicit val source: Row = row

      val countryCode = originalValue("COUNTRY_CODE")(row).get
      val sourceName = "EMAKINA"
      val sourceEntityId = originalValue("EM_SOURCE_ID")(row).get
      val concatId = createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
      val ohubCreated = currentTimestamp()

      // TODO: OPERATOR_REF_ID, what is this id?

      // format: OFF

                                                                              // ↓ not so happy with this column (it should be the same as the fieldName), macro?
      Operator(
        // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
        concatId                    = concatId                                                                                                           ,
        countryCode                 = mandatory ( "COUNTRY_CODE",             "countryCode"                                                             ), // TODO lookup country code
        isActive                    = true                                                                                                               ,
        isGoldenRecord              = false                                                                                                              ,
        ohubId                      = Option.empty                                                                                                       ,
        name                        = mandatory ( "OPERATOR_NAME",            "name"                                                                    ),
        sourceEntityId              = mandatory ( "EM_SOURCE_ID",             "sourceEntityId"                                                          ),
        sourceName                  = sourceName                                                                                                         ,
        ohubCreated                 = ohubCreated                                                                                                        ,
        ohubUpdated                 = ohubCreated                                                                                                        ,
        averagePrice                = None                                                                                                               ,
        chainId                     = None                                                                                                               ,
        chainName                   = None                                                                                                               ,
        channel                     = optional  ( "TYPE_OF_BUSINESS",         "channel"                                                                 ),
        city                        = None                                                                                                               ,
        cookingConvenienceLevel     = None                                                                                                               ,
        countryName                 = None                                                                                                               ,
        customerType                = None                                                                                                               , // TODO introduce when enum is available
        dateCreated                 = None                                                                                                               ,
        dateUpdated                 = None                                                                                                               ,
        daysOpen                    = None                                                                                                               ,
        distributorName             = optional  ( "PRIMARY_DISTRIBUTOR",      "distributorName"                                                         ),
        distributorOperatorId       = optional  ( "DISTRIBUTOR_CUSTOMER_ID",  "distributorOperatorId"                                                   ),
        emailAddress                = None                                                                                                               ,
        faxNumber                   = None                                                                                                               ,
        hasDirectMailOptIn          = None                                                                                                               ,
        hasDirectMailOptOut         = None                                                                                                               ,
        hasEmailOptIn               = None                                                                                                               ,
        hasEmailOptOut              = None                                                                                                               ,
        hasFaxOptIn                 = None                                                                                                               ,
        hasFaxOptOut                = None                                                                                                               ,
        hasGeneralOptOut            = None                                                                                                               ,
        hasMobileOptIn              = None                                                                                                               ,
        hasMobileOptOut             = None                                                                                                               ,
        hasTelemarketingOptIn       = None                                                                                                               ,
        hasTelemarketingOptOut      = None                                                                                                               ,
        houseNumber                 = None                                                                                                               ,
        houseNumberExtension        = None                                                                                                               ,
        isNotRecalculatingOtm       = None                                                                                                               ,
        isOpenOnFriday              = optional  ( "OPEN_ON_FRIDAY",           "isOpenOnFriday",               parseBoolUnsafe _                         ),
        isOpenOnMonday              = optional  ( "OPEN_ON_MONDAY",           "isOpenOnMonday",               parseBoolUnsafe _                         ),
        isOpenOnSaturday            = optional  ( "OPEN_ON_SATURDAY",         "isOpenOnSaturday",             parseBoolUnsafe _                         ),
        isOpenOnSunday              = optional  ( "OPEN_ON_SUNDAY",           "isOpenOnSunday",               parseBoolUnsafe _                         ),
        isOpenOnThursday            = optional  ( "OPEN_ON_THURSDAY",         "isOpenOnThursday",             parseBoolUnsafe _                         ),
        isOpenOnTuesday             = optional  ( "OPEN_ON_TUESDAY",          "isOpenOnTuesday",              parseBoolUnsafe _                         ),
        isOpenOnWednesday           = optional  ( "OPEN_ON_WEDNESDAY",        "isOpenOnWednesday",            parseBoolUnsafe _                         ),
        isPrivateHousehold          = optional  ( "PRIVATE_HOUSEHOLD",        "isPrivateHousehold",           parseBoolUnsafe _                         ),
        kitchenType                 = optional  ( "TYPE_OF_CUISINE",          "kitchenType"                                                             ),
        mobileNumber                = None                                                                                                               ,
        netPromoterScore            = None                                                                                                               ,
        oldIntegrationId            = None                                                                                                               ,
        otm                         = None                                                                                                               ,
        otmEnteredBy                = None                                                                                                               ,
        phoneNumber                 = None                                                                                                               ,
        region                      = None                                                                                                               ,
        salesRepresentative         = None                                                                                                               ,
        state                       = None                                                                                                               ,
        street                      = None                                                                                                               ,
        subChannel                  = None                                                                                                               ,
        totalDishes                 = optional  ( "NR_OF_COVERS_PER_DAY",     "totalDishes",                  parseNumberOrAverageFromRange _           ),
        totalLocations              = optional  ( "NR_OF_LOCATIONS",          "totalLocations",               parseNumberOrAverageFromRange _           ),
        totalStaff                  = optional  ( "NR_OF_KITCHEN_STAFF",      "totalStaff",                   parseNumberOrAverageFromRange _           ),
        vat                         = optional  ( "VAT",                      "vat"                                                                     ),
        webUpdaterId                = optional  ( "WEBUPDATER_ID",            "webUpdaterId"                                                            ),
        weeksClosed                 = None                                                                                                               ,
        zipCode                     = None                                                                                                               ,
        ingestionErrors             = errors
      )
    // format: ON
  }
}
