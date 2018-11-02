package com.unilever.ohub.spark.ingest.common

import java.util.UUID

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.ingest.CustomParsers._
import com.unilever.ohub.spark.ingest.{ DomainTransformer, ProductEmptyParquetWriter }
import org.apache.spark.sql.Row

object ProductConverter extends CommonDomainGateKeeper[Product] with ProductEmptyParquetWriter {

  override def toDomainEntity: DomainTransformer ⇒ Row ⇒ Product = { transformer ⇒ row ⇒
    import transformer._
    implicit val source: Row = row

    val concatId: String = createConcatId("countryCode", "sourceName", "sourceEntityId")
    val ohubCreated = currentTimestamp()

    // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

    // fieldName                        mandatory   sourceFieldName             targetFieldName           transformationFunction (unsafe)
    Product(
      concatId                        = concatId,
      countryCode                     = mandatory( "countryCode",              "countryCode"                                    ),
      customerType                    = Product.customerType                                                                     ,
      dateCreated                     = optional(  "dateCreated",              "dateCreated",            parseDateTimeUnsafe()  ),
      dateUpdated                     = optional(  "dateUpdated",              "dateUpdated",            parseDateTimeUnsafe()  ),
      isActive                        = mandatory( "isActive",                 "isActive",               toBoolean              ),
      isGoldenRecord                  = true,
      ohubId                          = Some(UUID.randomUUID().toString),
      name                            = mandatory( "name",                     "name"                                           ),
      sourceEntityId                  = mandatory( "sourceEntityId",           "sourceEntityId"),
      sourceName                      = mandatory( "sourceName",               "sourceName"),
      ohubCreated                     = ohubCreated,
      ohubUpdated                     = ohubCreated,
      // specific fields
      additives                       = List.empty,
      allergens                       = List.empty,
      availabilityHint                = Option.empty,
      benefits                        = Option.empty,
      brandCode                       = optional(  "brandCode",      "brandCode"),
      brandName                       = Option.empty,
      categoryByMarketeer             = Option.empty,
      categoryCodeByMarketeer         = Option.empty,
      categoryLevel1                  = Option.empty,
      categoryLevel2                  = Option.empty,
      categoryLevel3                  = Option.empty,
      categoryLevel4                  = Option.empty,
      categoryLevel5                  = Option.empty,
      categoryLevel6                  = Option.empty,
      categoryLevel7                  = Option.empty,
      categoryLevel8                  = Option.empty,
      categoryLevel9                  = Option.empty,
      categoryLevel10                 = Option.empty,
      categoryLevel11                 = Option.empty,
      categoryLevel12                 = Option.empty,
      categoryLevel13                 = Option.empty,
      code                            = optional(  "code",                    "code"),
      codeType                        = Some("MRDR"),
      consumerUnitLoyaltyPoints       = Option.empty,
      consumerUnitPriceInCents        = Option.empty,
      containerCode                   = Option.empty,
      containerName                   = Option.empty,
      currency                        = optional(  "currency",      "currency"),
      defaultPackagingType            = Option.empty,
      description                     = Option.empty,
      dietetics                       = List.empty,
      distributionUnitLoyaltyPoints   = Option.empty,
      distributionUnitPriceInCents    = Option.empty,
      eanConsumerUnit                 = optional(  "eanConsumerUnit",                  "eanConsumerUnit"),
      eanDistributionUnit             = optional(  "eanDistributionUnit",                  "eanDistributionUnit"),
      hasConsumerUnit                 = Option.empty,
      hasDistributionUnit             = Option.empty,
      imageId                         = Option.empty,
      ingredients                     = Option.empty,
      isAvailable                     = Option.empty,
      isDistributionUnitOnlyProduct   = Option.empty,
      isLoyaltyReward                 = Option.empty,
      isTopProduct                    = Option.empty,
      isUnileverProduct               = Option.empty,
      itemType                        = Option.empty,
      language                        = Option.empty,
      lastModifiedDate                = Option.empty,
      nameSlug                        = Option.empty,
      number                          = Option.empty,
      nutrientTypes                   = List.empty,
      nutrientValues                  = List.empty,
      orderScore                      = Option.empty,
      packagingCode                   = Option.empty,
      packagingName                   = Option.empty,
      packshotUrl                     = Option.empty,
      portionSize                     = Option.empty,
      portionUnit                     = Option.empty,
      preparation                     = Option.empty,
      productCodes                    = List.empty,
      productType                     = Option.empty,
      solutionCopy                    = Option.empty,
      subBrandCode                    = Option.empty,
      subBrandName                    = Option.empty,
      subCategoryByMarketeer          = Option.empty,
      subCategoryCode                 = Option.empty,
      subCategoryName                 = Option.empty,
      `type`                          = Some("Product"),
      unit                            = optional(  "unit",                    "unit"),
      unitPrice                       = optional(  "unitPrice",               "unitPrice",                 toBigDecimal ),
      youtubeUrl                      = Option.empty,
      // other fields
      additionalFields                = additionalFields,
      ingestionErrors                 = errors
    )
    // format: ON
  }
}
