package com.unilever.ohub.spark.domain.entity

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ProductDomainExportWriter extends DomainExportWriter[Product]

object Product extends DomainEntityCompanion {
  val customerType = "PRODUCT"
  override val engineFolderName = "products"
  override val excludedFieldsForCsvExport: Seq[String] = DomainEntityCompanion.defaultExcludedFieldsForCsvExport ++
    Seq("additives", "allergens", "dietetics", "nutrientTypes", "nutrientValues", "productCodes")
  override val domainExportWriter: Option[DomainExportWriter[Product]] = Some(ProductDomainExportWriter)
}

case class Product(
                    // generic fields
                    id: String,
                    creationTimestamp: Timestamp,
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
                    availabilityHint: Option[Long],
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
                    consumerUnitLoyaltyPoints: Option[Long],
                    consumerUnitPriceInCents: Option[Long],
                    containerCode: Option[String],
                    containerName: Option[String],
                    currency: Option[String],
                    defaultPackagingType: Option[String],
                    description: Option[String],
                    dietetics: List[String],
                    distributionUnitLoyaltyPoints: Option[Long],
                    distributionUnitPriceInCents: Option[Long],
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
                    isUnileverProduct: Option[String],
                    itemType: Option[String],
                    language: Option[String],
                    lastModifiedDate: Option[Timestamp],
                    nameSlug: Option[String],
                    number: Option[String],
                    nutrientTypes: List[String],
                    nutrientValues: List[String],
                    orderScore: Option[Long],
                    packagingCode: Option[String],
                    packagingName: Option[String],
                    packshotUrl: Option[String],
                    portionSize: Option[BigDecimal],
                    portionUnit: Option[String],
                    preparation: Option[String],
                    productCodes: List[String],
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
  override def getCompanion: DomainEntityCompanion = Product
}
