package com.unilever.ohub.spark.tsv2parquet.file_interface

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._
import org.apache.spark.sql.Row

object ProductConverter extends FileDomainGateKeeper[Product] {

  override def toDomainEntity: (Row, DomainTransformer) ⇒ Product = {
    (row, transformer) ⇒
      import transformer._
      implicit val source: Row = row

      val concatId: String = createConcatId("COUNTRY_CODE", "SOURCE", "REF_PRODUCT_ID")
      val ohubCreated = currentTimestamp()

      // format: OFF             // see also: https://stackoverflow.com/questions/3375307/how-to-disable-code-formatting-for-some-part-of-the-code-using-comments

      // fieldName                        mandatory   sourceFieldName             targetFieldName           transformationFunction (unsafe)
      Product(
        concatId                        = concatId,
        countryCode                     = mandatory( "COUNTRY_CODE",              "countryCode"                                       ),
        dateCreated                     = mandatory( "DATE_CREATED",              "dateCreated",            parseDateTimeStampUnsafe _),
        dateUpdated                     = mandatory( "DATE_MODIFIED",             "dateUpdated",            parseDateTimeStampUnsafe _),
        isActive                        = mandatory( "STATUS",                    "isActive",               parseBoolUnsafe _),
        isGoldenRecord                  = false,
        ohubId                          = Option.empty,
        name                            = mandatory( "PRODUCT_NAME",              "name"                                              ),
        sourceEntityId                  = mandatory( "REF_PRODUCT_ID",            "sourceEntityId"),
        sourceName                      = mandatory( "SOURCE",                    "sourceName"),
        ohubCreated                     = ohubCreated,
        ohubUpdated                     = ohubCreated,
        // specific fields
        additives                       = List.empty,
        allergens                       = List.empty,
        availabilityHint                = Option.empty,
        benefits                        = Option.empty,
        brandCode                       = Option.empty,
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
        code                            = optional(  "MRDR",                    "code"),
        codeType                        = Some("MRDR"),
        consumerUnitLoyaltyPoints       = Option.empty,
        consumerUnitPriceInCents        = Option.empty,
        containerCode                   = Option.empty,
        containerName                   = Option.empty,
        currency                        = optional(  "UNIT_PRICE_CURRENCY",      "currency"),
        defaultPackagingType            = Option.empty,
        description                     = Option.empty,
        dietetics                       = List.empty,
        distributionUnitLoyaltyPoints   = Option.empty,
        distributionUnitPriceInCents    = Option.empty,
        eanConsumerUnit                 = optional(  "EAN_CU",                  "eanConsumerUnit"),
        eanDistributionUnit             = optional(  "EAN_DU",                  "eanDistributionUnit"),
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
        unit                            = optional(  "UNIT",                    "unit"),
        unitPrice                       = optional(  "UNIT_PRICE",              "unitPrice",                 parseBigDecimalUnsafe _),
        youtubeUrl                      = Option.empty,
        // other fields
        ingestionErrors                 = errors
      )
    // format: ON
  }
}
