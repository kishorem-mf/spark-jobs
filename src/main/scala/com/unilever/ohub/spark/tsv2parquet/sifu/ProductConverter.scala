package com.unilever.ohub.spark.tsv2parquet.sifu

import java.util.UUID

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.tsv2parquet.{ DomainTransformer, ProductEmptyParquetWriter }
import com.unilever.ohub.spark.tsv2parquet.CustomParsers._

object ProductConverter extends SifuProductConverter with ProductEmptyParquetWriter {

  override protected[sifu] def sifuDataProvider: SifuDataProvider = new JsonSifuDataProvider()
}

trait SifuProductConverter extends SifuDomainGateKeeper[Product] {

  override protected[sifu] def sifuSelection: String = "products"

  override def toDomainEntity: DomainTransformer ⇒ SifuProductResponse ⇒ Product = { transformer ⇒ row ⇒
    import transformer._

    // format: OFF

    val sourceName         =   "SIFU"
    val countryCode        =   row.country.get.toUpperCase // todo convert
    val sourceEntityId     =   UUID.randomUUID().toString
    val concatId           =   DomainEntity.createConcatIdFromValues(countryCode, sourceName, sourceEntityId)
    val ohubCreated        =   currentTimestamp()

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
      name                            = mandatory("name", "name", row.name),
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
      imageId                         = row.image1Id,
      ingredients                     = row.ingredients,
      isAvailable                     = Some(row.available),
      isDistributionUnitOnlyProduct   = Some(row.duOnlyProduct),
      isLoyaltyReward                 = Some(row.loyaltyReward),
      isTopProduct                    = Some(row.topProduct),
      isUnileverProduct               = row.isUnileverProduct,
      itemType                        = row.itemType,
      language                        = row.language, // todo convert
      lastModifiedDate                = optional("lastModifiedDate", "lastModifiedDate", row.lastModifiedDate, parseDateTimeForPattern(dateTimePattern = sifuDate)),
      nameSlug                        = row.nameSlug,
      number                          = row.number,
      nutrientTypes                   = row.nutrientTypes.getOrElse(List.empty),
      nutrientValues                  = row.nutrientValues.getOrElse(List.empty),
      orderScore                      = optional("orderScore", "orderScore",row.orderScore, toLong),
      packagingCode                   = row.packagingCode,
      packagingName                   = row.packagingName,
      packshotUrl                     = row.packshotUrl,
      portionSize                     = optional("portionSize", "portionSize", row.portionSize, parseBigDecimalUnsafe), // todo convert
      portionUnit                     = row.portionUnit,
      preparation                     = row.preparation,
      productCodes                    = row.productCodes.getOrElse(List.empty),
      productId                       = Some(UUID.randomUUID().toString),
      productType                     = row.productType,
      solutionCopy                    = row.solutionCopy,
      subBrandCode                    = row.subBrandCode,
      subBrandName                    = row.subBrandName,
      subCategoryByMarketeer          = None,
      subCategoryCode                 = row.subCategoryCode,
      subCategoryName                 = row.subCategoryName,
      `type`                          = None,
      unit                            = None,
      unitPrice                       = None,
      youtubeUrl                      = row.youtubeUrl,
      // other fields
      additionalFields                = additionalFields,
      ingestionErrors                 = errors
    )
    // format: ON
  }
}
