package com.unilever.ohub.spark.export.ddl

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestProducts
import com.unilever.ohub.spark.export.ddl.model.DdlProducts

class ProductsDdlConverterSpec extends SparkJobSpec with TestProducts {
  val SUT = ProductsDdlConverter
  val productToConvert = defaultProduct.copy(ohubId = Some("12345"), codeType = Some("codeType"),
    name = "productName", code = Some("code"), eanDistributionUnit = Some("2345"),
    eanConsumerUnit = Some("1234"), subCategoryCode = Some("subCategoryCode"),
    categoryLevel13 = Some("categoryLevel13"), language = Some("eu"), dateUpdated = Some(Timestamp.valueOf("2015-06-30 13:49:00.0")))

  describe("Product ddl converter") {
    it("should convert a Products parquet correctly into an Orderline csv") {
      var result = SUT.convert(productToConvert)
      var expectedDdlProduct = DdlProducts(
        code = "source-entity-id",
        codeType = "codeType",
        salesOrganisationCode = "",
        distributionChannelCode = "",
        divisionCode = "",
        productType = "Loyalty",
        productName = "productName",
        productShortName = "",
        materialTypeCode = "",
        statusCode = "",
        baseUoMCode = "",
        pricingReferenceMaterialCode = "",
        materialCode = "code",
        comCode = "",
        csEuropeanArticleNumber = "2345",
        cuEuropeanArticleNumber = "1234",
        palletEuropeanArticleNumber = "",
        brandCode = "",
        subBrandCode = "",
        categoryCode = "",
        subCategoryCode = "subCategoryCode",
        eurVanguardHierarchyCode = "categoryLevel13",
        productPricingHierarchyLevel1Name = "",
        productPricingHierarchyLevel2Name = "",
        productPricingHierarchyLevel3Name = "",
        languageKeyCode = "eu",
        priceClassificationCode = "",
        volumeRebateGroupCode = "",
        pricingReferenceMaterialGroupCode = "",
        materialGroup2Code = "",
        materialGroup3Code = "",
        materialGroup5Code = "",
        consumerUnitperCaseValue = "",
        consumerUnitperPalletValue = "",
        caseperPalletValue = "",
        layersPerPalletValue = "",
        casesPerLayerValue = "",
        palletsPerCaseValue = "",
        netWeight = "",
        grossWeight = "",
        volume = "",
        length = "",
        width = "",
        height = "",
        caseHeight = "",
        caseLength = "",
        caseWidth = "",
        caseGrossWeight = "",
        consumerUnitHeight = "",
        consumerUnitLength = "",
        consumerUnitWidth = "",
        consumerUnitGrossWeight = "",
        palletLength = "",
        palletWidth = "",
        palletHeight = "",
        grossPalletWeight = "",
        sapLastUpdateDate = "2015-06-30 01:49:00:000"
      )
      result shouldBe expectedDdlProduct
    }
  }
}
