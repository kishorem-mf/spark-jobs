package com.unilever.ohub.spark.tsv2parquet.sifu

import java.util.UUID

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.tsv2parquet.DomainTransformer

object ProductConverter extends SifuDomainGateKeeper[Product] {

  override protected[sifu] def sifuSelection: String = "products"

  override def toDomainEntity: DomainTransformer ⇒ SifuProductResponse ⇒ Product = { transformer ⇒ row ⇒
    import transformer._

      // format: OFF

      val sourceName                                    =   "SIFU"
      val countryCode                                   =   row.country.get
      val sourceEntityId                                =   UUID.randomUUID().toString
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
        name                            = row.name.get,
        sourceEntityId                  = sourceEntityId,
        sourceName                      = sourceName,
        ohubCreated                     = ohubCreated,
        ohubUpdated                     = ohubCreated,
        // specific fields
        additives                       = row.additives.getOrElse(List.empty),
        allergens                       = row.allergens.getOrElse(List.empty),
        availabilityHint                = row.availabilityHint,
        benefits                        = row.benefits,
        brandCode                       = row.brandCode,
        brandName                       = row.brandName,
        categoryByMarketeer             = row.categoryCode,
        categoryCodeByMarketeer         = row.categoryName,
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
        code                            = row.code,
        codeType                        = Some("WEB"),
        consumerUnitLoyaltyPoints       = row.cuLoyaltyPoints,
        consumerUnitPriceInCents        = row.cuPriceInCents,
        containerCode                   = row.containerCode,
        containerName                   = row.containerName,
        currency                        = None,
        defaultPackagingType            = row.defaultPackagingType,
        description                     = row.description,
        dietetics                       = row.dietetics.getOrElse(List.empty),
        distributionUnitLoyaltyPoints   = row.duLoyaltyPoints,
        distributionUnitPriceInCents    = row.duPriceInCents,
        eanConsumerUnit                 = row.cuEanCode,
        eanDistributionUnit             = row.duEanCode,
        hasConsumerUnit                 = row.cuAvailable,
        hasDistributionUnit             = row.duAvailable,
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
