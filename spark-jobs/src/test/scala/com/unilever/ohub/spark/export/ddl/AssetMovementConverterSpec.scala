package com.unilever.ohub.spark.export.ddl

import java.sql.Timestamp

import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity.TestAssetMovements
import com.unilever.ohub.spark.export.ddl.model.DdlAssetMovements

class AssetMovementConverterSpec extends SparkJobSpec with TestAssetMovements {
  val SUT = AssetMovementDdlConverter
  val assetMovementToConvert = defaultAssetMovement.copy(ohubId = Some("12345"), crmId = Some("1"),
    operatorOhubId = Some("operatorOhubId"), assemblyDate = Some(Timestamp.valueOf("2015-06-30 13:49:00.0")),
    assetConcatId = Some("1~2~3"), createdBy = Some("Reshma"))

  describe("Asset Movement ddl converter") {
    it("should convert a Asset Movement parquet correctly into an Asset Movement csv") {
      val result = SUT.convert(assetMovementToConvert)

      val expectedDdlAssetMovement = DdlAssetMovements(
        ID = "1",
        Account = "operatorOhubId",
        `Assembly Date` = "2015-06-30 01:49:00:0",
        Asset = "1~2~3",
        CabinetCode = "b3a6208c-d7f6-44e2-80e2-f26d461f64c0",
        `Created By` = "Reshma",
        Currency = "",
        `Delivery Date` = "",
        `Last Modified By` = "",
        Name = "",
        Notes = "",
        Owner = "",
        Quantity = "",
        `Record Type` = "",
        `Return Date` = "",
        Status = ""
      )
      result shouldBe expectedDdlAssetMovement
    }
  }
}
