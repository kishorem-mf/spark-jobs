package com.unilever.ohub.spark.merging

import com.unilever.ohub.spark.SharedSparkSession.spark
import com.unilever.ohub.spark.SparkJobSpec
import com.unilever.ohub.spark.domain.entity._

class ChannelMappingMergingSpecs extends SparkJobSpec with TestChannelMappings {

  import spark.implicits._

  private val SUT = ChannelMappingMerging

  describe("Channelmapping merging") {
    it("should keep new non-existing record and set ohubId") {
      val delta = Seq(defaultChannelMapping).toDataset
      val integrated = Seq[ChannelMapping]().toDataset
      val result = SUT.transform(spark, delta, integrated).collect()

      result.length shouldBe 1
    }

    it("should add new non-existing record and set ohubId for new record") {
      val existingOhubId = Some("asdfg")

      val delta = Seq(defaultChannelMapping.copy(concatId = "delta")).toDataset
      val integrated = Seq[ChannelMapping](defaultChannelMapping.copy(concatId = "integrated", ohubId = existingOhubId)).toDataset
      val result = SUT.transform(spark, delta, integrated).orderBy($"concatId".asc).collect()

      result.length shouldBe 2
      result(0).ohubId should not be existingOhubId
      result(0).ohubId shouldBe defined
      result(1).ohubId shouldBe existingOhubId
    }

    it("should replace existing record and copy it's ohubId") {
      val existingOhubId = Some("asdfg")

      val oldLocalChannel = "oldChannel"
      val updatedLocalChannel = "newChannel"

      val delta = Seq(defaultChannelMapping.copy(concatId = "existing", localChannel = updatedLocalChannel)).toDataset
      val integrated = Seq[ChannelMapping](
        defaultChannelMapping.copy(concatId = "existing", ohubId = existingOhubId, localChannel = oldLocalChannel),
        defaultChannelMapping.copy(concatId = "dontMindMe")
      ).toDataset
      val result = SUT.transform(spark, delta, integrated).orderBy($"concatId".asc).collect()

      result.length shouldBe 2
      result(0).concatId shouldBe "dontMindMe"
      result(1).ohubId shouldBe existingOhubId
      result(1).localChannel shouldBe updatedLocalChannel
    }
  }
}
