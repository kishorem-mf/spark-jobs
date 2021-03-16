package com.unilever.ohub.spark.export.ddl

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestAssets
import com.unilever.ohub.spark.export.ddl.model.DdlAssets

class AssetDdlConverterSpec extends SparkJobSpec with TestAssets {
  val SUT = AssetDdlConverter
  val assetToConvert = defaultAsset.copy(crmId = Some("id-1"), ohubId = Some("12345"), name = Some("name"), `type` = Some("type"),
    brandName = Some("brandName"), description = Some("description"), dimensions = Some("dimensions"),
    numberOfTimesRepaired = Some(2), powerConsumption = Some("11A"), numberOfCabinetBaskets = Some(1),
    numberOfCabinetFacings = Some(1), serialNumber = Some("2"))

  describe("Asset ddl converter") {
    it("should convert a asset parquet correctly into an asset csv") {
      val result = SUT.convert(assetToConvert)
      val expectedAsset = DdlAssets(
        ID = "id-1",
        `Asset Name` = "name",
        `Asset Record Type` = "type",
        `Cabinet Branding` = "brandName",
        `Cabinet Code` = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        `Description` = "description",
        `External ID` = "DE~EMAKINA~b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        `Measures (L x W x H)` = "dimensions",
        `Number of Times Repaired` = "2",
        `Power Consumption` = "11A",
        `Quantity of Baskets (IE/GW)` = "1",
        `Quantity of Facings` = "1",
        `Serial Number` = "2",
        `Status (OOH)` = "",
        Status = "",
        `Useful Capacity (l)` = "",
        `UDL Timestamp` = "",
        `FOL or Sold` = "",
        `Task Relation` = ""
      )
      result shouldBe expectedAsset
    }
  }
}
