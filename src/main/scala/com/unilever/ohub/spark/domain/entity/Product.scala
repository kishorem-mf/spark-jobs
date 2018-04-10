package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity
import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.constraint._

case class Product(
    // generic fields
    concatId: String, // concatenation of: countryCode ~ sourceName ~ sourceEntityId (entity identifier)
    countryCode: String, // TODO Existing country code in OHUB using: Iso 3166-1 alpha 2
    dateCreated: Timestamp,
    dateUpdated: Timestamp,
    isActive: Boolean,
    isGoldenRecord: Boolean, // does this make sense here / in DomainEntity?
    ohubId: Option[String],
    name: String,
    sourceEntityId: String,
    sourceName: String,
    ohubCreated: Timestamp,
    ohubUpdated: Timestamp, // currently always created timestamp (how/when will it get an updated timestamp?)
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
    productType: Option[String],
    solutionCopy: Option[String],
    subBrandCode: Option[String],
    subBrandName: Option[String],
    subCategoryCode: Option[String],
    subCategoryName: Option[String],
    unit: Option[String],
    unitPrice: Option[Float],
    youtubeUrl: Option[String],
    // other fields
    ingestionErrors: Map[String, IngestionError]
) extends DomainEntity {
}
