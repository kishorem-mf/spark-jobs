package com.unilever.ohub.spark.tsv2parquet.web_event

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer
import org.apache.spark.sql.Row

object OperatorConverter extends WebEventDomainGateKeeper[Operator] {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Operator = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val countryCode = mandatoryValue("countryCode", "countryCode")(row)
    val sourceEntityId = mandatoryValue("operatorRefId", "sourceEntityId")(row)
    val concatId = DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
    val ohubCreated = currentTimestamp()

    // format: OFF

    Operator(
      // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      concatId                    = concatId                                                                                                           ,
      countryCode                 = countryCode                                                                                                        ,
      customerType                = Operator.customerType                                                                                              ,
      dateCreated                 = Option.empty                                                                                                       ,
      dateUpdated                 = Option.empty                                                                                                       ,
      isActive                    = true                                                                                                               ,
      isGoldenRecord              = false                                                                                                              ,
      ohubId                      = None                                                                                                               ,
      name                        = mandatory ( "operatorName",           "name"                                                                      ),
      sourceEntityId              = sourceEntityId                                                                                                     ,
      sourceName                  = sourceName                                                                                                         ,
      ohubCreated                 = ohubCreated                                                                                                        ,
      ohubUpdated                 = ohubCreated                                                                                                        ,
      averagePrice                = None                                                                                                               ,
      chainId                     = None                                                                                                               ,
      chainName                   = None                                                                                                               ,
      channel                     = optional  ( "typeOfBusiness",         "channel"                                                                   ),
      city                        = optional  ( "businessAddress.city",   "city"                                                                      ),
      cookingConvenienceLevel     = None                                                                                                               ,
      countryName                 = countryName(countryCode)                                                                                           ,
      // countryName                 = optional  ( "businessAddress.country", "countryName"                                                              ),
      daysOpen                    = None                                                                                                               ,
      distributorName             = optional  ( "primaryDistributor",     "distributorName"                                                           ),
      distributorOperatorId       = optional  ( "distributorCustomerId",  "distributorOperatorId"                                                     ),
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
      houseNumber                 = optional  ( "businessAddress.houseNumber",  "houseNumber"                                                         ),
      houseNumberExtension        = optional  ( "businessAddress.houseNumberExtension",  "houseNumberExtension"                                       ),
      isNotRecalculatingOtm       = None                                                                                                               ,
      isOpenOnFriday              = optional  ( "openOnFriday",           "isOpenOnFriday",               parseBoolUnsafe                             ),
      isOpenOnMonday              = optional  ( "openOnMonday",           "isOpenOnMonday",               parseBoolUnsafe                             ),
      isOpenOnSaturday            = optional  ( "openOnSaturday",         "isOpenOnSaturday",             parseBoolUnsafe                             ),
      isOpenOnSunday              = optional  ( "openOnSunday",           "isOpenOnSunday",               parseBoolUnsafe                             ),
      isOpenOnThursday            = optional  ( "openOnThursday",         "isOpenOnThursday",             parseBoolUnsafe                             ),
      isOpenOnTuesday             = optional  ( "openOnTuesday",          "isOpenOnTuesday",              parseBoolUnsafe                             ),
      isOpenOnWednesday           = optional  ( "openOnWednesday",        "isOpenOnWednesday",            parseBoolUnsafe                             ),
      isPrivateHousehold          = optional  ( "privateHousehold",       "isPrivateHousehold",           parseBoolUnsafe                             ),
      kitchenType                 = optional  ( "typeOfCuisine",          "kitchenType"                                                               ),
      mobileNumber                = None                                                                                                               ,
      netPromoterScore            = None                                                                                                               ,
      oldIntegrationId            = None                                                                                                               ,
      otm                         = None                                                                                                               ,
      otmEnteredBy                = None                                                                                                               ,
      phoneNumber                 = None                                                                                                               ,
      region                      = None                                                                                                               ,
      salesRepresentative         = None                                                                                                               ,
      state                       = optional  ( "businessAddress.state",  "state"                                                                     ),
      street                      = optional  ( "businessAddress.street", "street"                                                                    ),
      subChannel                  = None                                                                                                               ,
      totalDishes                 = optional  ( "numberOfCoversPerDay",   "totalDishes",                    parseNumberOrAverageFromRange             ),
      totalLocations              = optional  ( "numberOfLocations",      "totalLocations",                 parseNumberOrAverageFromRange             ),
      totalStaff                  = optional  ( "numberOfKitchenStaff",   "totalStaff",                     parseNumberOrAverageFromRange             ),
      vat                         = optional  ( "VAT",                    "vat"                                                                       ),
      webUpdaterId                = None                                                                                                               ,
      weeksClosed                 = None                                                                                                               ,
      zipCode                     = optional  ( "businessAddress.postCode",  "zipCode"                                                                ),
      additionalFields            = additionalFields                                                                                                   ,
      ingestionErrors             = errors
    )
    // format: ON
  }
}
