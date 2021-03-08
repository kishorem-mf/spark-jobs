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
        Code = "source-entity-id",
        CodeType = "codeType",
        SalesOrganisationCode = "",
        DistributionChannelCode = "",
        DivisionCode = "",
        ProductType = "Loyalty",
        ProductName = "productName",
        ProductShortName = "",
        MaterialTypeCode = "",
        StatusCode = "",
        BaseUoMCode = "",
        PricingReferenceMaterialCode = "",
        MaterialCode = "code",
        COMCode = "",
        CSEuropeanArticleNumber = "2345",
        CUEuropeanArticleNumber = "1234",
        PalletEuropeanArticleNumber = "",
        BrandCode = "",
        SubBrandCode = "",
        CategoryCode = "",
        SubCategoryCode = "subCategoryCode",
        EURVanguardHierarchyCode = "categoryLevel13",
        ProductPricingHierarchyLevel1Name = "",
        ProductPricingHierarchyLevel2Name = "",
        ProductPricingHierarchyLevel3Name = "",
        LanguageKeyCode = "eu",
        PriceClassificationCode = "",
        VolumeRebateGroupCode = "",
        PricingReferenceMaterialGroupCode = "",
        MaterialGroup2Code = "",
        MaterialGroup3Code = "",
        MaterialGroup5Code = "",
        ConsumerUnitperCaseValue = "",
        ConsumerUnitperPalletValue = "",
        CaseperPalletValue = "",
        LayersPerPalletValue = "",
        CasesPerLayerValue = "",
        PalletsPerCaseValue = "",
        NetWeight = "",
        GrossWeight = "",
        Volume = "",
        Length = "",
        WIDTH = "",
        HEIGHT = "",
        CaseHeight = "",
        CaseLength = "",
        CaseWidth = "",
        CaseGrossWeight = "",
        ConsumerUnitHeight = "",
        ConsumerUnitLength = "",
        ConsumerUnitWidth = "",
        ConsumerUnitGrossWeight = "",
        PalletLength = "",
        PalletWidth = "",
        PalletHeight = "",
        GrossPalletWeight = "",
        SAPLastUpdateDate = "2015-06-30 01:49:00:000"
      )
      result shouldBe expectedDdlProduct
    }
  }
}
