package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.AssetMovement
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class AssetMovementConverterSpec extends CsvDomainGateKeeperSpec[AssetMovement] {

  override val SUT = AssetMovementConverter

  describe("common AssetMovement converter") {
    it("should convert an AssetMovement correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_ASSETMOVEMENT.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualAssetMovement = actualDataSet.head()

        val expectedAssetMovement = AssetMovement(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "DE~EMAKINA~123",
          countryCode = "DE",
          customerType = "ASSETMOVEMENT",
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          isActive = true,
          isGoldenRecord = false,
          sourceEntityId = "123",
          sourceName = "EMAKINA",
          ohubId = Option.empty,
          ohubCreated = actualAssetMovement.ohubCreated,
          ohubUpdated = actualAssetMovement.ohubUpdated,

          crmId = None,
          operatorOhubId = None,
          operatorConcatId = Some("DE~EMAKINA~123"),
          assemblyDate = None,
          assetConcatId = None,
          createdBy = None,
          currency = None,
          deliveryDate = None,
          lastModifiedBy = None,
          name = None,
          comment = None,
          owner = None,
          quantityOfUnits = None,
          `type` = None,
          returnDate = None,
          assetStatus = None,

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualAssetMovement shouldBe expectedAssetMovement
      }
    }
  }
}
