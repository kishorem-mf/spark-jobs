package com.unilever.ohub.spark.domain.entity

import java.sql.{Timestamp}

import com.unilever.ohub.spark.domain.DomainEntity.IngestionError
import com.unilever.ohub.spark.domain.{DomainEntity, DomainEntityCompanion}
import com.unilever.ohub.spark.export.ExportOutboundWriter
import com.unilever.ohub.spark.export.azuredw.{AzureDWWriter, ProductDWWriter}
import com.unilever.ohub.spark.export.domain.DomainExportWriter

object ProductDomainExportWriter extends DomainExportWriter[Product]

object Product extends DomainEntityCompanion[Product] {
  val customerType = "PRODUCT"
  override val auroraFolderLocation = None
  override val engineFolderName = "products"
  override val excludedFieldsForCsvExport: Seq[String] = DomainEntityCompanion.defaultExcludedFieldsForCsvExport ++
    Seq("additives", "allergens", "dietetics", "nutrientTypes", "nutrientValues", "productCodes")
  override val defaultExcludedFieldsForParquetExport: Seq[String] = DomainEntityCompanion.defaultExcludedFieldsForParquetExport ++
    Seq("lastModifiedDate")
  override val domainExportWriter: Option[DomainExportWriter[Product]] = Some(ProductDomainExportWriter)
  override val acmExportWriter: Option[ExportOutboundWriter[Product]] = Some(com.unilever.ohub.spark.export.acm.ProductOutboundWriter)
  override val dispatchExportWriter: Option[ExportOutboundWriter[Product]] = Some(com.unilever.ohub.spark.export.dispatch.ProductOutboundWriter)
  override val azureDwWriter: Option[AzureDWWriter[Product]] = Some(ProductDWWriter)
  override val auroraExportWriter: Option[ExportOutboundWriter[Product]] = Some(com.unilever.ohub.spark.export.aurora.ProductOutboundWriter)
}

case class ProductSifu(
                      brandCode:Option[String],
                      brandName:Option[String],
                      ingredients:Option[String],
                      language:Option[String],
                      lastModifiedDate:Option[Timestamp],
                      subBrandName:Option[String],
                      youtubeUrl:Option[String],
                      productNumber:Option[String],
                      productCode:Option[String],
                      duEanCode:Option[String],
                      cuEanCode:Option[String],
                      country:Option[String],
                      packagingName:Option[String],
                      productType:Option[String],
                      url:Option[String],
                      loyaltyReward:Option[Boolean],
                      categoryCode:Option[String],
                      categoryName:Option[String],
                      subCategoryCode:Option[String],
                      subCategoryName:Option[String],
                      subBrandCode: Option[String],
                      packagingCode:Option[String],
                      image1Id:Option[String],
                      packshotUrl:Option[String],
                      allergens: List[String],
                      nutrientTypes: List[String],
                      nutrientValues: List[String],
                      cuPriceInCents: Option[Long],
                      duPriceInCents: Option[Long],
                      packshotResizedUrl:Option[String],
                      code:Option[String],
                      dachClaimFooterTexts:List[String],
                      name:Option[String],
                      number:Option[String],
                      productName:Option[String],
                      categoryCodes_1:Option[String],
                      itemType:Option[String],
                      subCategoryCodes_1:Option[String],
                      isUnileverProduct: Option[String],
                      subCategoryNames_1:Option[String],
                      convenienceLevel:Option[String],
                      extractedImageUrl:Option[String],
                      extractedPreviewImageUrl:Option[String],
                      extractedImageId:Option[String],
                      extractedNutritionalType: List[String],
                      extractedNutritionalValue: List[String],
                      extractedPer100gAsPrep: Option[String],
                      per100gAsPrepOffPackUnitOfMeasurement: Option[String],
                      description: Option[String]
) extends scala.Product

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
                    ingestionErrors: Map[String, IngestionError],
                    url: Option[String],
                    previewImageUrl: Option[String],
                    imageUrl: Option[String],
                    convenienceLevel: Option[String]

                  ) extends DomainEntity {
  override def getCompanion: DomainEntityCompanion[Product] = Product
}
