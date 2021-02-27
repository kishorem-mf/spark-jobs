package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.domain.entity.Product
import com.unilever.ohub.spark.export.{Converter, TypeConversionFunctions}
import com.unilever.ohub.spark.export.ddl.model.DdlProducts

object ProductsDdlConverter extends Converter[Product, DdlProducts] with TypeConversionFunctions {
  // scalastyle:off method.length
  override def convert(implicit product: Product, explain: Boolean = false): DdlProducts = {
    DdlProducts(
      code = getValue("sourceEntityId"),
      codeType = getValue("codeType"),
      salesOrganisationCode = Option.empty,
      distributionChannelCode = Option.empty,
      divisionCode = Option.empty,
      productType = getValue("type"),
      productName = getValue("name"),
      productShortName = Option.empty,
      materialTypeCode = Option.empty,
      statusCode = Option.empty,
      baseUoMCode = Option.empty,
      pricingReferenceMaterialCode = Option.empty,
      materialCode = getValue("code"),
      comCode = Option.empty,
      csEuropeanArticleNumber = getValue("eanDistributionUnit"),
      cuEuropeanArticleNumber = getValue("eanConsumerUnit"),
      palletEuropeanArticleNumber = Option.empty,
      brandCode = getValue("brandCode"),
      subBrandCode = getValue("subBrandCode"),
      categoryCode = Option.empty,
      subCategoryCode = getValue("subCategoryCode"),
      eurVanguardHierarchyCode = getValue("categoryLevel13"),
      productPricingHierarchyLevel1Name = Option.empty,
      productPricingHierarchyLevel2Name = Option.empty,
      productPricingHierarchyLevel3Name = Option.empty,
      languageKeyCode = getValue("language"),
      priceClassificationCode = Option.empty,
      volumeRebateGroupCode = Option.empty,
      pricingReferenceMaterialGroupCode = Option.empty,
      materialGroup2Code = Option.empty,
      materialGroup3Code = Option.empty,
      materialGroup5Code = Option.empty,
      consumerUnitperCaseValue = Option.empty,
      consumerUnitperPalletValue = Option.empty,
      caseperPalletValue = Option.empty,
      layersPerPalletValue = Option.empty,
      casesPerLayerValue = Option.empty,
      palletsPerCaseValue = Option.empty,
      netWeight = Option.empty,
      grossWeight = Option.empty,
      volume = Option.empty,
      length = Option.empty,
      width = Option.empty,
      height = Option.empty,
      caseHeight = Option.empty,
      caseLength = Option.empty,
      caseWidth = Option.empty,
      caseGrossWeight = Option.empty,
      consumerUnitHeight = Option.empty,
      consumerUnitLength = Option.empty,
      consumerUnitWidth = Option.empty,
      consumerUnitGrossWeight = Option.empty,
      palletLength = Option.empty,
      palletWidth = Option.empty,
      palletHeight = Option.empty,
      grossPalletWeight = Option.empty,
      sapLastUpdateDate = getValue("dateUpdated")
    )
  }
}
