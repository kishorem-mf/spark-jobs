package com.unilever.ohub.spark.ingest.common

import java.sql.Timestamp

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.domain.entity.ChannelMapping
import com.unilever.ohub.spark.ingest.CsvDomainGateKeeperSpec

class ChannelMappingConverterSpec extends CsvDomainGateKeeperSpec[ChannelMapping] {

  override val SUT = ChannelMappingConverter

  describe("common channelMapping converter") {
    import spark.implicits._

    it("should convert a channelMapping correctly from a valid common csv input") {
      val inputFile = "src/test/resources/COMMON_CHANNEL_MAPPINGS.csv"

      runJobWith(inputFile) { actualDataSet ⇒
        actualDataSet.count() shouldBe 4

        val actualChannelMapping = actualDataSet.orderBy($"creationTimestamp".asc).head()

        val expectedChannelMapping = ChannelMapping(
          id = "3af22c48-e78e-4487-9cfa-a42e192b041f",
          creationTimestamp = new Timestamp(1556708917626L),
          concatId = "AD~mysource~Hotel/Alojamiento",
          countryCode = "AD",
          customerType = "OPERATOR",
          sourceEntityId = "Hotel/Alojamiento",
          sourceName = "mysource",
          isActive = true,
          ohubCreated = actualChannelMapping.ohubCreated,
          ohubUpdated = actualChannelMapping.ohubUpdated,
          dateCreated = None,
          dateUpdated = None,
          ohubId = Option.empty,
          isGoldenRecord = true,

          originalChannel = "Hotel/Alojamiento",
          localChannel = "Hotels",
          channelUsage = "Key",
          channelReference = "-1",

          additionalFields = Map(),
          ingestionErrors = Map()
        )
        actualChannelMapping shouldBe expectedChannelMapping
      }
    }
  }
}
