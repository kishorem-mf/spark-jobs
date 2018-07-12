package com.unilever.ohub.spark.ingest.web_event

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.Operator
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, OperatorEmptyParquetWriter }
import org.apache.spark.sql.Row
import cats.syntax.option._

object OperatorConverter extends WebEventDomainGateKeeper[Operator] with OperatorEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Operator = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val countryCode = mandatoryValue("countryCode", "countryCode")(row)
    val sourceEntityId = mandatoryValue("operatorRefId", "sourceEntityId")(row)
    val concatId = DomainEntity.createConcatIdFromValues(countryCode, SourceName, sourceEntityId)
    val ohubCreated = currentTimestamp()

    // format: OFF

    Operator(
      // fieldName                  mandatory   sourceFieldName             targetFieldName                 transformationFunction (unsafe)
      concatId                    = concatId                                                                                                           ,
      countryCode                 = countryCode                                                                                                        ,
      customerType                = Operator.customerType                                                                                              ,
      dateCreated                 = none                                                                                                       ,
      dateUpdated                 = none                                                                                                       ,
      isActive                    = true                                                                                                               ,
      isGoldenRecord              = false                                                                                                              ,
      ohubId                      = none                                                                                                               ,
      name                        = mandatory ( "operatorName",           "name"                                                                      ),
      sourceEntityId              = sourceEntityId                                                                                                     ,
      sourceName                  = SourceName                                                                                                         ,
      ohubCreated                 = ohubCreated                                                                                                        ,
      ohubUpdated                 = ohubCreated                                                                                                        ,
      averagePrice                = none                                                                                                               ,
      chainId                     = none                                                                                                               ,
      chainName                   = none                                                                                                               ,
      channel                     = optional  ( "typeOfBusiness",         "channel"                                                                   ),
      city                        = optional  ( "businessAddress.city",   "city"                                                                      ),
      cookingConvenienceLevel     = none                                                                                                               ,
      countryName                 = countryName(countryCode),
      daysOpen                    = none                                                                                                               ,
      distributorName             = optional  ( "primaryDistributor",     "distributorName"                                                           ),
      distributorOperatorId       = optional  ( "distributorCustomerId",  "distributorOperatorId"                                                     ),
      emailAddress                = none                                                                                                               ,
      faxNumber                   = none                                                                                                               ,
      hasDirectMailOptIn          = none                                                                                                               ,
      hasDirectMailOptOut         = none                                                                                                               ,
      hasEmailOptIn               = none                                                                                                               ,
      hasEmailOptOut              = none                                                                                                               ,
      hasFaxOptIn                 = none                                                                                                               ,
      hasFaxOptOut                = none                                                                                                               ,
      hasGeneralOptOut            = none                                                                                                               ,
      hasMobileOptIn              = none                                                                                                               ,
      hasMobileOptOut             = none                                                                                                               ,
      hasTelemarketingOptIn       = none                                                                                                               ,
      hasTelemarketingOptOut      = none                                                                                                               ,
      houseNumber                 = optional  ( "businessAddress.houseNumber",  "houseNumber"                                                         ),
      houseNumberExtension        = optional  ( "businessAddress.houseNumberExtension",  "houseNumberExtension"                                       ),
      isNotRecalculatingOtm       = none                                                                                                               ,
      isOpenOnFriday              = optional  ( "openOnFriday",           "isOpenOnFriday",               parseBoolUnsafe                             ),
      isOpenOnMonday              = optional  ( "openOnMonday",           "isOpenOnMonday",               parseBoolUnsafe                             ),
      isOpenOnSaturday            = optional  ( "openOnSaturday",         "isOpenOnSaturday",             parseBoolUnsafe                             ),
      isOpenOnSunday              = optional  ( "openOnSunday",           "isOpenOnSunday",               parseBoolUnsafe                             ),
      isOpenOnThursday            = optional  ( "openOnThursday",         "isOpenOnThursday",             parseBoolUnsafe                             ),
      isOpenOnTuesday             = optional  ( "openOnTuesday",          "isOpenOnTuesday",              parseBoolUnsafe                             ),
      isOpenOnWednesday           = optional  ( "openOnWednesday",        "isOpenOnWednesday",            parseBoolUnsafe                             ),
      isPrivateHousehold          = optional  ( "privateHousehold",       "isPrivateHousehold",           parseBoolUnsafe                             ),
      kitchenType                 = optional  ( "typeOfCuisine",          "kitchenType"                                                               ),
      mobileNumber                = none                                                                                                               ,
      netPromoterScore            = none                                                                                                               ,
      oldIntegrationId            = none                                                                                                               ,
      otm                         = none                                                                                                               ,
      otmEnteredBy                = none                                                                                                               ,
      phoneNumber                 = none                                                                                                               ,
      region                      = none                                                                                                               ,
      salesRepresentative         = none                                                                                                               ,
      state                       = optional  ( "businessAddress.state",  "state"                                                                     ),
      street                      = optional  ( "businessAddress.street", "street"                                                                    ),
      subChannel                  = none                                                                                                               ,
      totalDishes                 = optional  ( "numberOfCoversPerDay",   "totalDishes",                    parseNumberOrAverageFromRange             ),
      totalLocations              = optional  ( "numberOfLocations",      "totalLocations",                 parseNumberOrAverageFromRange             ),
      totalStaff                  = optional  ( "numberOfKitchenStaff",   "totalStaff",                     parseNumberOrAverageFromRange             ),
      vat                         = optional  ( "VAT",                    "vat"                                                                       ),
      webUpdaterId                = none                                                                                                               ,
      weeksClosed                 = none                                                                                                               ,
      zipCode                     = optional  ( "businessAddress.postCode",  "zipCode"                                                                ),
      additionalFields            = additionalFields                                                                                                   ,
      ingestionErrors             = errors
    )

    // format: ON
  }
}
