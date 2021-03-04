package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.domain.entity.Asset
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class AssetConverterSpec extends CsvDomainGateKeeperSpec[Asset] {

  override val SUT = AssetConverter

  describe("common asset converter") {
    it("should convert an asset correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_ASSET.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 1

        val actualAsset = actualDataSet.head()

        val expectedAsset = Asset(
          id = "id-1",
          creationTimestamp = new Timestamp(1542205922011L),
          concatId = "DE~EMAKINA~123",
          countryCode = "DE",
          customerType = "ASSET",
          dateCreated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          dateUpdated = Some(Timestamp.valueOf("2017-12-18 10:33:54.0")),
          isActive = true,
          isGoldenRecord = false,
          sourceEntityId = "123",
          sourceName = "EMAKINA",
          ohubId = Option.empty,
          ohubCreated = actualAsset.ohubCreated,
          ohubUpdated = actualAsset.ohubUpdated,

          crmId = None,
          name =  None,
          brandName =  None,
          `type` = None,
          description =  None,
          dimensions =  None,
          numberOfTimesRepaired =  None,
          powerConsumption =  None,
          numberOfCabinetBaskets =  None,
          numberOfCabinetFacings =  None,
          serialNumber =  None,
          oohClassification =  None,
          lifecyclePhase =  None,
          capacityInLiters =  None,
          dateCreatedInUdl =  None,
          leasedOrSold =  None,
          crmTaskId =  None,
          assemblyDate =  None,

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualAsset shouldBe expectedAsset
      }
    }
  }
}
