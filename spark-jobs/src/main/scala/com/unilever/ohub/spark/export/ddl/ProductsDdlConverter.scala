package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}
import com.unilever.ohub.spark.export.ddl.model.DdlProducts

object ProductsDdlConverter extends Converter[Product, DdlProducts] with TypeConversionFunctions {
  // scalastyle:off method.length
  override def convert(implicit product: Product, explain: Boolean = false): DdlProducts = {
    DdlProducts(
      Code = getValue("sourceEntityId"),
      CodeType = getValue("codeType"),
      SalesOrganisationCode = Option.empty,
      DistributionChannelCode = Option.empty,
      DivisionCode = Option.empty,
      ProductType = getValue("type"),
      ProductName = getValue("name"),
      ProductShortName = Option.empty,
      MaterialTypeCode = Option.empty,
      StatusCode = Option.empty,
      BaseUoMCode = Option.empty,
      PricingReferenceMaterialCode = Option.empty,
      MaterialCode = getValue("code"),
      COMCode = Option.empty,
      CSEuropeanArticleNumber = getValue("eanDistributionUnit"),
      CUEuropeanArticleNumber = getValue("eanConsumerUnit"),
      PalletEuropeanArticleNumber = Option.empty,
      BrandCode = getValue("brandCode"),
      SubBrandCode = getValue("subBrandCode"),
      CategoryCode = Option.empty,
      SubCategoryCode = getValue("subCategoryCode"),
      EURVanguardHierarchyCode = getValue("categoryLevel13"),
      ProductPricingHierarchyLevel1Name = Option.empty,
      ProductPricingHierarchyLevel2Name = Option.empty,
      ProductPricingHierarchyLevel3Name = Option.empty,
      LanguageKeyCode = getValue("language"),
      PriceClassificationCode = Option.empty,
      VolumeRebateGroupCode = Option.empty,
      PricingReferenceMaterialGroupCode = Option.empty,
      MaterialGroup2Code = Option.empty,
      MaterialGroup3Code = Option.empty,
      MaterialGroup5Code = Option.empty,
      ConsumerUnitperCaseValue = Option.empty,
      ConsumerUnitperPalletValue = Option.empty,
      CaseperPalletValue = Option.empty,
      LayersPerPalletValue = Option.empty,
      CasesPerLayerValue = Option.empty,
      PalletsPerCaseValue = Option.empty,
      NetWeight = Option.empty,
      GrossWeight = Option.empty,
      Volume = Option.empty,
      Length = Option.empty,
      WIDTH = Option.empty,
      HEIGHT = Option.empty,
      CaseHeight = Option.empty,
      CaseLength = Option.empty,
      CaseWidth = Option.empty,
      CaseGrossWeight = Option.empty,
      ConsumerUnitHeight = Option.empty,
      ConsumerUnitLength = Option.empty,
      ConsumerUnitWidth = Option.empty,
      ConsumerUnitGrossWeight = Option.empty,
      PalletLength = Option.empty,
      PalletWidth = Option.empty,
      PalletHeight = Option.empty,
      GrossPalletWeight = Option.empty,
      SAPLastUpdateDate = getValue("dateUpdated")
    )
  }
}
