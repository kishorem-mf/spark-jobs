package com.unilever.ohub.spark.tsv2parquet.sifu

import java.util.UUID

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.tsv2parquet.{DomainGateKeeper, DomainTransformer}
import org.apache.spark.sql.Row

object ProductConverter extends SifuDomainGateKeeper[Product] {

  override def toDomainEntity: (Row, DomainTransformer) ⇒ Product = {
    (row, transformer) ⇒
      import transformer._
      implicit val source: Row = row

      // format: OFF

      val sourceName                                    =   "SIFU"
      val countryCode                                   =   originalValue(SALES_ORG)(row).get // postgres
      val sourceEntityId                                =   originalValue(MAT_UID)(row).get
      val productId                                     =   UUID.randomUUID().toString
      val concatId                                      =   DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
      val ohubCreated                                   =   currentTimestamp()

      Product(
        // fieldName                  mandatory   sourceFieldName           targetFieldName                 transformationFunction (unsafe)
        concatId                        = concatId,
        countryCode                     = countryCode,
        customerType                    = Product.customerType,
        dateCreated                     = None,
        dateUpdated                     = None,
        isActive                        = true,
        isGoldenRecord                  = true,
        ohubId                          = Some(UUID.randomUUID().toString),
        name                            = mandatory( NAME_1,                 "name"),
        sourceEntityId                  = sourceEntityId,
        sourceName                      = sourceName,
        ohubCreated                     = ohubCreated,
        ohubUpdated                     = ohubCreated,
        // specific fields
        additives                       = List.empty,
        allergens                       = List.empty,
        availabilityHint                = None,
        benefits                        = None,
        brandCode                       = None,
        brandName                       = None,
        categoryByMarketeer             = None,
        categoryCodeByMarketeer         = None,
        categoryLevel1                  = None,
        categoryLevel2                  = None,
        categoryLevel3                  = None,
        categoryLevel4                  = None,
        categoryLevel5                  = None,
        categoryLevel6                  = None,
        categoryLevel7                  = None,
        categoryLevel8                  = None,
        categoryLevel9                  = None,
        categoryLevel10                 = None,
        categoryLevel11                 = None,
        categoryLevel12                 = None,
        categoryLevel13                 = None,
        code                            = optional(  PRODUCT_MRDR,        "code"),
        codeType                        = Some("MRDR"),
        consumerUnitLoyaltyPoints       = None,
        consumerUnitPriceInCents        = None,
        containerCode                   = None,
        containerName                   = None,
        currency                        = None,
        defaultPackagingType            = Option.empty,
        description                     = Option.empty,
        dietetics                       = List.empty,
        distributionUnitLoyaltyPoints   = Option.empty,
        distributionUnitPriceInCents    = Option.empty,
        eanConsumerUnit                 = None,
        eanDistributionUnit             = optional(  EAN,                  "eanDistributionUnit"),
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
        productId                       = Some(UUID.randomUUID().toString),
        productType                     = Option.empty,
        solutionCopy                    = Option.empty,
        subBrandCode                    = Option.empty,
        subBrandName                    = Option.empty,
        subCategoryByMarketeer          = Option.empty,
        subCategoryCode                 = Option.empty,
        subCategoryName                 = Option.empty,
        `type`                          = None,
        unit                            = None,
        unitPrice                       = None,
        youtubeUrl                      = Option.empty,
        // other fields
        additionalFields                = additionalFields,
        ingestionErrors                 = errors
      )
    // format: ON
  }
}
