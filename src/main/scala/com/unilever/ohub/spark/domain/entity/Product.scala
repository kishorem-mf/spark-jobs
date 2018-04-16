package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError

object Product {
  val customerType = "product"
}

case class Product(
    // generic fields
    concatId: String,
    countryCode: String,
    customerType: String,
    dateCreated: Option[Timestamp],
    dateUpdated: Option[Timestamp],
    isActive: Boolean,
    isGoldenRecord: Boolean,
    ohubId: Option[String],
    name: String,
    sourceEntityId: String,
    sourceName: String,
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp,
    // specific fields
    additives: List[String],
    allergens: List[String],
    availabilityHint: Option[Int],
    benefits: Option[String],
    brandCode: Option[String],
    brandName: Option[String],
    categoryByMarketeer: Option[String],
    categoryCodeByMarketeer: Option[String],
    categoryLevel1: Option[String],
    categoryLevel2: Option[String],
    categoryLevel3: Option[String],
    categoryLevel4: Option[String],
    categoryLevel5: Option[String],
    categoryLevel6: Option[String],
    categoryLevel7: Option[String],
    categoryLevel8: Option[String],
    categoryLevel9: Option[String],
    categoryLevel10: Option[String],
    categoryLevel11: Option[String],
    categoryLevel12: Option[String],
    categoryLevel13: Option[String],
    code: Option[String],
    codeType: Option[String],
    consumerUnitLoyaltyPoints: Option[Int],
    consumerUnitPriceInCents: Option[Int],
    containerCode: Option[String],
    containerName: Option[String],
    currency: Option[String],
    defaultPackagingType: Option[String],
    description: Option[String],
    dietetics: List[String],
    distributionUnitLoyaltyPoints: Option[Int],
    distributionUnitPriceInCents: Option[Int],
    eanConsumerUnit: Option[String],
    eanDistributionUnit: Option[String],
    hasConsumerUnit: Option[Boolean],
    hasDistributionUnit: Option[Boolean],
    imageId: Option[String],
    ingredients: Option[String],
    isAvailable: Option[Boolean],
    isDistributionUnitOnlyProduct: Option[Boolean],
    isLoyaltyReward: Option[Boolean],
    isTopProduct: Option[Boolean],
    isUnileverProduct: Option[Boolean],
    itemType: Option[String],
    language: Option[String],
    lastModifiedDate: Option[Timestamp],
    nameSlug: Option[String],
    number: Option[String],
    nutrientTypes: List[String],
    nutrientValues: List[String],
    orderScore: Option[Int],
    packagingCode: Option[String],
    packagingName: Option[String],
    packshotUrl: Option[String],
    portionSize: Option[Float],
    portionUnit: Option[String],
    preparation: Option[String],
    productCodes: List[String],
    productId: Option[String],
    productType: Option[String],
    solutionCopy: Option[String],
    subBrandCode: Option[String],
    subBrandName: Option[String],
    subCategoryByMarketeer: Option[String],
    subCategoryCode: Option[String],
    subCategoryName: Option[String],
    `type`: Option[String],
    unit: Option[String],
    unitPrice: Option[BigDecimal],
    youtubeUrl: Option[String],
    // other fields
    additionalFields: Map[String, String],
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity {
  // TODO refine...what's the minimal amount of constraints needed before a product should be accepted
}
